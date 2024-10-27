package tw.com.hanjiCHEN.auditservice.products.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductEventDto;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductEventType;
import tw.com.hanjiCHEN.auditservice.events.dto.SnsMessageDto;
import tw.com.hanjiCHEN.auditservice.products.repositories.ProductEventsRepository;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class ProductEventsConsumer {
    private static final Logger LOG = LogManager.getLogger(ProductEventsConsumer.class);
    private final ObjectMapper objectMapper;
    private final SqsAsyncClient sqsAsyncClient;
    private final String productEventsQueueUrl;
    private final ReceiveMessageRequest receiveMessageRequest;
    private final ProductEventsRepository productEventsRepository;

    @Autowired
    public ProductEventsConsumer(ObjectMapper objectMapper,
                                 SqsAsyncClient sqsAsyncClient,
                                 @Value("${aws.sqs.queue.product.event.url}") String productEventsQueueUrl,
                                 ProductEventsRepository productEventsRepository) {

        this.objectMapper = objectMapper;
        this.sqsAsyncClient = sqsAsyncClient;
        this.productEventsQueueUrl = productEventsQueueUrl;

        this.receiveMessageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(5) //每次從queue中最多取出多少訊息處理
                .queueUrl(productEventsQueueUrl)
                .build();
        this.productEventsRepository = productEventsRepository;
    }

    @Scheduled(fixedDelay = 1000)//每秒執行一次
    public void receiveProductEventsMessage() {
        List<Message> messages;
        while (!(messages = sqsAsyncClient.receiveMessage(receiveMessageRequest).join().messages()).isEmpty()) {
            LOG.info("Reading {} messages", messages.size());
            messages.parallelStream().forEach(message -> {
                try {
                    SnsMessageDto snsMessageDto = objectMapper.readValue(message.body(), SnsMessageDto.class);
                    String requestId = snsMessageDto.messageAttributes().requestId().value();
                    String messageId = snsMessageDto.messageId();
                    String traceId = snsMessageDto.messageAttributes().traceId().value();

                    ThreadContext.put("messageId", messageId);
                    ThreadContext.put("requestId", requestId);
                    ProductEventType eventType = ProductEventType
                            .valueOf(snsMessageDto.messageAttributes().eventType().value());

                    CompletableFuture<Void> productEventFuture;
                    switch (eventType) {
                        case PRODUCT_CREATED, PRODUCT_UPDATED, PRODUCT_DELETED -> {
                            ProductEventDto productEventDto =
                                    objectMapper.readValue(snsMessageDto.message(), ProductEventDto.class);

                            productEventFuture = productEventsRepository.create(productEventDto, eventType,
                                    messageId, requestId, traceId);

                            LOG.info("Product event: {} - Id: {}", eventType, productEventDto.id());
                        }
                        default -> {
                            LOG.error("Invalid product event:{}", eventType);
                            throw new Exception("Invalid product event");
                        }
                    }

                    CompletableFuture<DeleteMessageResponse> deleteMessageResponseCompletableFuture =
                            sqsAsyncClient.deleteMessage(DeleteMessageRequest.builder()
                                    .queueUrl(productEventsQueueUrl)
                                    .receiptHandle(message.receiptHandle())
                                    .build());

                    CompletableFuture.allOf(productEventFuture,deleteMessageResponseCompletableFuture).join();

                    LOG.info("Message deleted...");
                } catch (Exception e) {
                    LOG.error("Failed to parse product event message");
                    throw new RuntimeException(e);
                } finally {
                    ThreadContext.clearAll();
                }
            });
        }
    }
}
