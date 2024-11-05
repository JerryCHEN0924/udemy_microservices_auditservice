package tw.com.hanjiCHEN.auditservice.products.dto;

import tw.com.hanjiCHEN.auditservice.products.models.ProductEvent;

public record ProductEventApiDto(
        String productId,
        String code,
        float price,
        String requestId,
        String email,
        long createAt
) {
    public ProductEventApiDto (ProductEvent productEvent) {
        this(
                productEvent.getInfo().getId(),
                productEvent.getInfo().getCode(),
                productEvent.getInfo().getPrice(),
                productEvent.getInfo().getRequestId(),
                productEvent.getEmail(),
                productEvent.getCreatedAt()
        );
    }
}
