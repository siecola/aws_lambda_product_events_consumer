package br.com.siecola.aws_lambda_product_events_consumer.handler;

import br.com.siecola.aws_lambda_product_events_consumer.model.ProductEvent;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;

public class ProductEventsHandler implements RequestHandler<SQSEvent, Void> {
    private static final String DYNAMODB_TABLE_NAME = "product-events";

    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        LambdaLogger logger = context.getLogger();

        for (SQSEvent.SQSMessage msg : input.getRecords()) {
            Gson gson = new Gson();
            ProductEvent productEvent = gson.fromJson(msg.getBody(), ProductEvent.class);

            logger.log("Product event received - messageId: " + msg.getMessageId() + " - productId: " +
                    productEvent.getProductId() + " - event: " + productEvent.getEvent());

            saveProductEvent(msg.getMessageId(), productEvent);
        }
        return null;
    }


    private void saveProductEvent(String messageId, ProductEvent productEvent) {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDb = new DynamoDB(client);

        dynamoDb.getTable(DYNAMODB_TABLE_NAME)
                .putItem(new PutItemSpec().withItem(new Item().withString("id", messageId)
                        .withNumber("productId", productEvent.getProductId())
                        .withString("code", productEvent.getCode())
                        .withString("event", productEvent.getEvent())
                        .withLong("timestamp", productEvent.getTimestamp())
                ));
    }
}