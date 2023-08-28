<style>body {text-align: justify}</style>

# The Problem Description

During the migration of the monolithic application into a set of microservices, we introduced the RabbitMQ broker in its architecture to satisfy our needs for transparent separation of asynchronous scenarios. It worked well and allowed us to greatly simplify the main, most time-critical application and improve response times of the synchronous services that got rid of all potentially asynchronous code. However, during the migration of one of these services, we faced the need to deal with external services that experienced temporary outages. This required us to implement some kind of retry logic. Making it more complex was the requirement for delays to increase with the retry number (exponential backoff). The external service could be down for a couple of minutes to days, and we shouldn't discard these messages. At the same time, we should not overload gateways with constant retries.
Overall, RabbitMQ and Spring are well-integrated and flexible platforms, so we had many potential options for retries.

# Spring Retry
The first and most obvious option was Spring Retry (https://github.com/spring-projects/spring-retry), which is quite mature and simple to use. It already has plenty of options for retry configuration out of the box:
        org.springframework.amqp.rabbit.config.RetryInterceptorBuilder
                .stateless()
                .maxAttempts(5)
                .backOffOptions(1000, 10, 500000)
                .build();

However, it was rejected as it implements client-side delays and basically blocks the executing thread during retry delays. Furthermore, its stateless implementation may lead to lost messages if the node stops working. Its stateful implementation requires manual implementation, making it more complex and ineffective in a cluster setup.

# RabbitMQ Delayed Message Plugin (available for RabbitMQ 3.8.16+)

**https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/**

This was another option to implement retries. The idea behind it is simple: each message may have an "x-delay" header that is utilized by RabbitMQ's new "x-delayed-type" exchange flag to postpone the message publication. This solution is also quite stable, and it's even supported in Spring AMQP (since 1.6) through the Message.getMessageProperties().setDelay(Integer) method.

![image alt text](/assets/image_0.png)

However, it also has some substantial functional, performance, and message durability limitations for us:
* Delayed messages are not observable in the admin, limiting monitoring and alert possibilities.
* Delayed messages are not replicated between the broker nodes, so they may be lost with one of the nodes.
* The number of delayed messages should be kept as low as possible to avoid inefficient usage of broker resources.

# Message TTL Headers and Dead Letter Queues

So, we looked at the RabbitMQ native solution to the problem: dead letter queues (DLQs) and "time to live" (TTL) headers.
The simplest solution (at first glance) is to set per-message TTL headers and put them into a "wait" queue that is bound to the actual destination queue. When a message expires, it is automatically redirected to this queue.

![image alt text](/assets/image_1.png)

While this works fine, there's an important limitation: queue messages are processed sequentially, so it only works correctly for equal message delays. This means that a message with a bigger TTL "blocks" messages with shorter TTLs that were sent later on.
Since we had to implement more sophisticated retry delays, this limitation needed to be overcome. The solution here is to use multiple "wait" queues for different TTLs. There are some described ways to implement this by creating "wait" queues for all possible delays for each delayed queue, but it results in an explosion of the queue count and makes it hardly scalable for utilization in multiple queues.
So, we decided to implement a slightly more sophisticated version that shares delay queues.

![image alt text](/assets/image_2.png)

The first step was to separate the cases when a retry is actually needed. For that reason, we introduced the RetryException, so the client logic can identify such cases and wrap those errors into this exception class.
In the message listener code any exceptions that should result in retry should be wrapped into the RetryException and re-thrown:

```java
   @RabbitHandler
   public void processMessage(@Payload SomeMessage message) {

       ...

       try {

           externalService.call(request);

       } catch (ExternalServiceException e) {

           throw new RetryException(e);

       }
       ...
   }
```

Then we introduced the RetryPolicy interface that encapsulates all retry details:

```java
public interface RetryPolicy {
   String getQueue();
   Integer getDelay(int retries);
   int getRetriesLimit();
   Set<Integer> getRetryDelays();
}

```

Its implementations are trivial, even for exponential backoff cases:


```java
@Getter
@AllArgsConstructor
public class FixedDelayRetryPolicy implements RetryPolicy {
   private String queue;
   private int retriesLimit;
   private int delay;

   @Override
   public Integer getDelay(int retry) {
       return retry < retriesLimit ? delay : null;
   }

   @Override
   public Set<Integer> getRetryDelays() {
       return Set.of(delay);
   }
}

@Getter
@AllArgsConstructor
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
   private String queue;
   private int retriesLimit;
   private float backoffCoeff;
   private int startDelay;
   private int maximalDelay;

   @Override
   public Integer getDelay(int retry) {
       return retry < retriesLimit
               ? getDelay(retry)
               : null;
   }

   @Override
   public Set<Integer> getRetryDelays() {
       return IntStream.range(0, retriesLimit)
               .map(this::getDelay)
               .boxed()
               .collect(Collectors.toSet());
   }

   private int getDelay(int retry) {
       return (int) Math.min(maximalDelay, startDelay * Math.pow(backoffCoeff, retry));
   }

}
```

We also introduced custom message headers for the delay, retry number, and original queue name

```java
public abstract class AmqpMessageHeaders {
   public static final String RETURN_TO = "return-to";
   public static final String WAIT_FOR = "wait-for";
   public static final String RETRIES = "x-message-retries";

}
```

Now we have two more pieces left: ErrorHandler to handle RetryExceptions and exchanges/queues/bindings configuration.
The ErrorHandler implementation is quite straightforward:
It checks the exception type, finds the RetryPolicy based on the queue name, and acquires the required delay from the policy for the current retry number (from the message header). If the required retry count is not exceeded, then the three message headers mentioned above are added (or updated), and the message is sent to the wait exchange.

```java
public class AmqpListenerErrorHandler implements ErrorHandler {
   private final AmqpTemplate amqpTemplate;
   private String waitMessageExchange;
   private Map<String, RetryPolicy> retryPolicies;

   public ListenerErrorHandler(AmqpTemplate amqpTemplate, String waitMessageExchange, Set<RetryPolicy> retryPolicies) {
       this.amqpTemplate = amqpTemplate;
       this.waitMessageExchange = waitMessageExchange;
       this.retryPolicies = retryPolicies.stream()
               .collect(Collectors.toMap(p -> p.getQueue(), Function.identity()));
   }

   @Override
   public void handleError(Throwable t) {
       if (t instanceof ListenerExecutionFailedException) {
           ListenerExecutionFailedException exception = (ListenerExecutionFailedException) t;
           Message message = exception.getmessage();
           if (exception.getCause() instanceof RetryException) {
               log.debug("Failed to process the emessage: ", e.getCause());
               MessageProperties messageProperties = message.getMessageProperties();
               RetryPolicy retryPolicy = retryPolicies.get(messageProperties.getConsumerQueue());
               int retry = getRetry(message);
               Integer delay = retryPolicy != null
                       ? retryPolicy.getDelay(retry)
                       : null;

               if (delay == null) {
                   log.debug("Retries limit exceeded for message with id={}", messageProperties.getMessageId());
                   throw new AmqpRejectAndDontRequeueException("Retries limit exceeded", t);
               }

               Map<String, Object> headers = messageProperties.getHeaders();
               headers.put(AmqpMessageHeaders.RETURN_TO, messageProperties.getConsumerQueue());
               headers.put(AmqpMessageHeaders.WAIT_FOR, delay.toString());
               headers.put(AmqpMessageHeaders.RETRIES, retry + 1);
               amqpTemplate.convertAndSend(waitMessageExchange, messageProperties.getReceivedRoutingKey(), message);

               throw new ImmediateAcknowledgeAmqpException("Re-queued a message for retry");

           }

           log.error("Permanent error during processing the message: {}", message, t);

       } else {
           log.error("Permanent error during processing the message:", t);
       }

       throw new AmqpRejectAndDontRequeueException(t);
   }

   private static int getRetry(Message message) {
       return Objects.requireNonNullElse(message.getMessageProperties().getHeader(MessageHeaders.RETRIES), 0);
   }

}
```

This listener should be added to the RabbitListenerContainerFactory configuration:


```java
   @Bean
   public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(Set<RetryPolicy> retryPolicies) {

       SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
       ...
       factory.setErrorHandler(new AmqpListenerErrorHandler(amqpTemplate, AmqpNames.WAIT_EXCHANGE, retryPolicies));
       factory.setDefaultRequeueRejected(false);

       return factory;

   }
```
Also, note the "factory.setDefaultRequeueRejected(false)" part that prevents messages from automatical re-queue.
To create and wire all exchanges and queues, we used the usual Spring AMQP bean configurations that automatically create delay queues and bind them to the wait exchange:

```java
   @Bean
   public Declarables retryBindings(Set<RetryPolicy> retryPolicies) {
       HeadersExchange returnMessageExchange = new HeadersExchange(AmqpNames.RETURN_EXCHANGE);
       HeadersExchange waitMessageExchange = new HeadersExchange(AmqpNames.WAIT_EXHANGE);
       Set<Integer> delays = retryPolicies.stream()
               .flatMap(p -> p.getRetryDelays().stream())
               .collect(Collectors.toSet());

       Map<Integer, Queue> waitMessageQueues = delays.stream()
               .collect(Collectors.toMap(Function.identity(), d -> QueueBuilder
                       .durable(AmqpNames.WAIT_QUEUE_PREFIX + d)
                       .ttl(d)
                       .deadLetterExchange(AmqpNames.RETURN_EXCHANGE)
                       .build()));

       Set<Binding> waitMessageQueueBindings = waitMessageQueues.entrySet().stream()
               .map(e -> bind(e.getValue()).to(waitMessageExchange).where(MessageHeaders.WAIT_FOR).matches(e.getKey().toString()))
               .collect(Collectors.toSet());

       Set<Declarable> retryDeclarables = new HashSet<>();
       declarables.add(returnMessageExchange);
       declarables.add(waitMessageExchange);
       declarables.addAll(waitMessageQueues.values());
       declarables.addAll(waitMessageQueueBindings);

       return new Declarables(retryDeclarables);

   }
```

Here, all possible delays are collected from registered retry policies.
So, now for any new queue that should be retried you only need to define its RetryPolicy bean, and bind the queue to the return exchange.
All missing delay queues will be created and bound automatically:

```java

   @Bean
   public Declarables someQueueBindings() {
       TopicExchange someExchange = new TopicExchange(AmqpNames.SOME_EXCHANGE);
       Queue someQueue = QueueBuilder
               .durable(AmqpNames.SOME_QUEUE)
               .deadLetterExchange(AmqpNames.DL_EXCHANGE)
               .build();

       return new Declarables(
               someExchange,
               someQueue,
               bind(someQueue).to(returnExchange)
                   .where(MessageHeaders.RETURN_TO_HEADER)
                   .matches(AmqpNames.SOME_QUEUE)
       );
   }

   @Bean
   public RetryPolicy someRetryPolicy() {
       return new ExponentialBackoffRetryPolicy(AmqpNames.SOME_QUEUE, 5, 1000, 10.0f, 500000);
   }
```

This configures a queue with 5 retries, starting with 1 second, and making subsequent delays 10 times longer, but limiting them to not exceed 500 seconds (1s, 10s, 100s, 500s, and 500s).

# Takeaways
In a developed software technical landscape, the same technical problem could be solved in various ways, most of which may turn out to be suboptimal or even wrong, depending on the functional or non-functional requirements. It takes time and experience to carefully evaluate these options before starting to implement them. However, it always pays off in the long run with smooth releases and happy customers.

