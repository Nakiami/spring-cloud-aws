package org.springframework.cloud.aws.messaging.listener;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.junit.Test;
import org.springframework.cloud.aws.core.support.documentation.RuntimeUse;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.messaging.MessagingException;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NonBlockingMessageListenerContainerTest {

	private final int TEST_WAIT_TIMEOUT = 10;

	@Test
	public void testNonBlocking_shouldConsumerMoreMessagesOffQueueEvenThoughWorkerThreadIsBlocked() throws Exception {
		final int totalNumMessages = 3; // total messages to process
		final int sqsMaxNumMessages = 2; // messages to process at any one time

		final String queueName = "testQueue";
		final String queueUrl = "http://testSimpleReceiveMessage.amazonaws.com";

		final AtomicInteger processingOrderCounter = new AtomicInteger();
		final CountDownLatch totalCountDown = new CountDownLatch(totalNumMessages);
		final CountDownLatch resumeFirstMessageCountDown = new CountDownLatch(totalNumMessages - 1);
		final ArrayBlockingQueue messageProcessingCompletionOrder = new ArrayBlockingQueue(totalNumMessages);

		// Container
		final NonBlockingMessageListenerContainer container = createMessageListenerSkeleton();
		container.setMaxNumberOfMessages(sqsMaxNumMessages);

		// Message handler
		final QueueMessageHandler messageHandler = new QueueMessageHandler() {
			@Override
			public void handleMessage(org.springframework.messaging.Message<?> message) throws MessagingException {
				// the order in which this message started processing
				int messageProcessingStartOrder = processingOrderCounter.incrementAndGet();
				// if this is the first message to be processed, wait for the last message to finish
				if (messageProcessingStartOrder == 1) {
					try {
						synchronized (resumeFirstMessageCountDown) {
							resumeFirstMessageCountDown.await(TEST_WAIT_TIMEOUT, SECONDS);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// the order in which this message finished processing
				messageProcessingCompletionOrder.add(messageProcessingStartOrder);
				// count down from totalNumMessages to make sure we process all messages
				totalCountDown.countDown();
				// count down to when the first message processing should be resumed (when it is the only remaining message to be processed)
				resumeFirstMessageCountDown.countDown();
			}
		};
		container.setMessageHandler(messageHandler);

		final StaticApplicationContext applicationContext = new StaticApplicationContext();
		applicationContext.registerSingleton("testMessageListener", TestMessageListener.class);
		messageHandler.setApplicationContext(applicationContext);
		messageHandler.afterPropertiesSet();

		// SQS Mocks
		mockGetQueueUrl(container.getAmazonSqs(), queueName, queueUrl);
		mockGetQueueAttributesWithEmptyResult(container.getAmazonSqs(), queueUrl);
		when(container
				.getAmazonSqs()
				.receiveMessage(randomReceiveMessageRequestMatcher(queueUrl)))
				.thenReturn(randomReceiveMessageResult())
				.thenReturn(randomReceiveMessageResult())
				.thenReturn(randomReceiveMessageResult())
				.thenReturn(new ReceiveMessageResult()); // default message to return
		when(container
				.getAmazonSqs()
				.getQueueAttributes(any(GetQueueAttributesRequest.class)))
				.thenReturn(new GetQueueAttributesResult());

		// Initialize & start listener
		container.afterPropertiesSet();
		container.start();

		// Wait for all messages to process
		assertTrue(totalCountDown.await(TEST_WAIT_TIMEOUT, SECONDS));

		/* Make sure the messages got processed in the expected order..
		  - Two messages get picked off the SQS queue and enter processing. There are now no available
		  	processing slots since sqsMaxNumMessages is set to 2.
 		  - Message 1 goes to sleep, and as such will always finish last.
		  - Message 2 finishes processing immediately and triggers an increment and notify on availableSlots.
		  - Since availableSlots is now 1, another message gets picked off the SQS queue. It enter the message
		  	handler as Message 3, and finishes immediately.
		  - The order will as such always be 2, 3, 1
		*/
		assertEquals(2, messageProcessingCompletionOrder.remove());
		assertEquals(3, messageProcessingCompletionOrder.remove());
		assertEquals(1, messageProcessingCompletionOrder.remove());

		container.stop();
	}

	private static ReceiveMessageResult randomReceiveMessageResult() {
		return new ReceiveMessageResult()
				.withMessages(new Message()
						.withBody(UUID.randomUUID().toString())
						.withReceiptHandle(UUID.randomUUID().toString()));
	}

	private static ReceiveMessageRequest randomReceiveMessageRequestMatcher(String queueUrl) {
		return new ReceiveMessageRequest(queueUrl)
				.withAttributeNames("All")
				.withMaxNumberOfMessages(anyInt())
				.withMessageAttributeNames("All");
	}

	private static NonBlockingMessageListenerContainer createMessageListenerSkeleton() {
		NonBlockingMessageListenerContainer container = new NonBlockingMessageListenerContainer();
		container.setAutoStartup(true);
		container.setMaxNumberOfMessages(10);
		container.setVisibilityTimeout(60);
		container.setWaitTimeOut(20);
		container.setBackOffTime(30000);
		container.setAmazonSqs(mock(AmazonSQSAsync.class));
		return container;
	}

	private static void mockGetQueueAttributesWithEmptyResult(AmazonSQSAsync sqs, String queueUrl) {
		when(sqs.getQueueAttributes(new GetQueueAttributesRequest(queueUrl).withAttributeNames(QueueAttributeName.RedrivePolicy))).
				thenReturn(new GetQueueAttributesResult());
	}

	private static void mockGetQueueUrl(AmazonSQSAsync sqs, String queueName, String queueUrl) {
		when(sqs.getQueueUrl(new GetQueueUrlRequest(queueName))).thenReturn(new GetQueueUrlResult().
				withQueueUrl(queueUrl));
	}

	private static class TestMessageListener {

		private String message;
		private final CountDownLatch countDownLatch = new CountDownLatch(1);

		@RuntimeUse
		@SqsListener("testQueue")
		private void handleMessage(String message) {
			this.message = message;
			this.countDownLatch.countDown();
		}

		public String getMessage() {
			return this.message;
		}

		public CountDownLatch getCountDownLatch() {
			return this.countDownLatch;
		}
	}
}
