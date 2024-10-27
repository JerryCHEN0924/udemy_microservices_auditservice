package tw.com.hanjiCHEN.auditservice.products.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductEventDto;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductEventType;
import tw.com.hanjiCHEN.auditservice.events.dto.ProductFailureEventDto;
import tw.com.hanjiCHEN.auditservice.products.models.ProductEvent;
import tw.com.hanjiCHEN.auditservice.products.models.ProductFailureEvent;
import tw.com.hanjiCHEN.auditservice.products.models.ProductInfoEvent;
import tw.com.hanjiCHEN.auditservice.products.models.ProductInfoFailureEvent;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Repository
public class ProductFailureEventsRepository {
    private static final Logger LOG = LogManager.getLogger(ProductFailureEventsRepository.class);
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<ProductFailureEvent> productFailureEventsTable;

    @Autowired
    public ProductFailureEventsRepository(@Value("${aws.events.ddb}") String eventsDdbName,
                                          DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.productFailureEventsTable = dynamoDbEnhancedAsyncClient.table(eventsDdbName,
                TableSchema.fromBean(ProductFailureEvent.class));
    }

    public CompletableFuture<Void> create(ProductFailureEventDto productFailureEventDto,
                                          ProductEventType productEventType,
                                          String messageId, String requestId, String traceId) {
        long timestamp = Instant.now().toEpochMilli();
        long ttl = Instant.now().plusSeconds(300).getEpochSecond(); //time to live,現實案例可以改長一點時間,注意:此時間並不準確

        ProductFailureEvent productFailureEvent = new ProductFailureEvent();
        productFailureEvent.setPk("#product_".concat(productEventType.name()));//#product_PRODUCT_FAILURE
        productFailureEvent.setSk(String.valueOf(timestamp));
        productFailureEvent.setCreatedAt(timestamp);
        productFailureEvent.setTtl(ttl);
        productFailureEvent.setEmail(productFailureEventDto.email());

        ProductInfoFailureEvent productInfoFailureEvent = new ProductInfoFailureEvent();
        productInfoFailureEvent.setId(productFailureEventDto.id());
        productInfoFailureEvent.setMessageId(messageId);
        productInfoFailureEvent.setRequestId(requestId);
        productInfoFailureEvent.setTraceId(traceId);
        productInfoFailureEvent.setError(productFailureEventDto.error());
        productInfoFailureEvent.setStatus(productFailureEventDto.status());

        productFailureEvent.setInfo(productInfoFailureEvent);

        return productFailureEventsTable.putItem(productFailureEvent);
    }
}
