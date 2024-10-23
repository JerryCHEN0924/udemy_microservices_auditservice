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
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductEventType;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductFailureEventDto;
import tw.com.hanjiCHEN.auditservice.events.dto.SnsMessageDto;

import java.util.List;

@Service
public class ProductFailureEventsConsumer {
    private static final Logger LOG = LogManager.getLogger(ProductFailureEventsConsumer.class);
    private final ObjectMapper objectMapper;
    private final SqsAsyncClient sqsAsyncClient;
    private final String productFailureEventsQueueUrl;
    private final ReceiveMessageRequest receiveMessageRequest;

    @Autowired
    public ProductFailureEventsConsumer(ObjectMapper objectMapper,
                                        SqsAsyncClient sqsAsyncClient,
                                        @Value("${aws.sqs.queue.product.failure.events.url}")
                                        String productFailureEventsQueueUrl) {

        this.objectMapper = objectMapper;
        this.sqsAsyncClient = sqsAsyncClient;
        this.productFailureEventsQueueUrl = productFailureEventsQueueUrl;

        this.receiveMessageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(10) //每次從queue中最多取出多少訊息處理
                .queueUrl(productFailureEventsQueueUrl)
                .build();
    }

    @Scheduled(fixedDelay = 5000)//每5秒執行一次
    public void receiveProductFailureEventsMessage() {
        List<Message> messages;
        while (!(messages = sqsAsyncClient.receiveMessage(receiveMessageRequest).join().messages()).isEmpty()) {
            LOG.info("Reading {} messages", messages.size());
            messages.parallelStream().forEach(message -> {
                try {
                    SnsMessageDto snsMessageDto = objectMapper.readValue(message.body(), SnsMessageDto.class);
                    ThreadContext.put("messageId", snsMessageDto.messageId());
                    ThreadContext.put("requestId", snsMessageDto.messageAttributes().requestId().value());
                    ProductEventType eventType = ProductEventType
                            .valueOf(snsMessageDto.messageAttributes().eventType().value());

                    if (ProductEventType.PRODUCT_FAILURE == eventType) {
                        ProductFailureEventDto productEventDto =
                                objectMapper.readValue(snsMessageDto.message(), ProductFailureEventDto.class);
                        LOG.info("Product failure event: {} - Id: {}", eventType, productEventDto.id());
                    } else {
                        LOG.error("Invalid product failure event:{}", eventType);
                        throw new Exception("Invalid product failure event");
                    }

                    sqsAsyncClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(productFailureEventsQueueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build()).join();
                    LOG.info("Message deleted...");

                } catch (Exception e) {
                    LOG.error("Failed to parse product failure event message");
                    throw new RuntimeException(e);
                } finally {
                    ThreadContext.clearAll();
                }
            });
        }
    }
}
