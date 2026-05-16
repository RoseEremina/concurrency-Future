package com.eremin;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class DataAggregator {
    private final Random random = new Random();
    private final ExecutorService executor;

    public DataAggregator() {
        this.executor = Executors.newFixedThreadPool(3);
    }

    public CompletableFuture<ProductInfo> aggregateProductInfoAsync(String productName) {
        CompletableFuture<Double> priceFuture = fetchPriceAsync(productName);
        CompletableFuture<String> descriptionFuture = fetchDescriptionAsync(productName);
        CompletableFuture<Double> ratingFuture = fetchRatingAsync(productName);

        return CompletableFuture.allOf(priceFuture, descriptionFuture, ratingFuture)
                .thenApply(v -> {
                    try {
                        Double price = priceFuture.get();
                        String description = descriptionFuture.get();
                        Double rating = ratingFuture.get();

                        return new ProductInfo(productName, price, description, rating);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to aggregate product info", e);
                    }
                });
    }

    public ProductInfo aggregateProductInfo(String productName) {
        try {
            return aggregateProductInfoAsync(productName).get();
        } catch (Exception e) {
            return new ProductInfo(productName, 0.0, "Нет данных", 0.0);
        }
    }

    private CompletableFuture<Double> fetchPriceAsync(String productName) {
        return CompletableFuture.supplyAsync(() -> fetchPrice(productName), executor)
                .exceptionally(ex -> {
                    System.err.println("Error fetching price for " + productName + ": " + ex.getMessage());
                    return 0.0;
                });
    }

    private CompletableFuture<String> fetchDescriptionAsync(String productName) {
        return CompletableFuture.supplyAsync(() -> fetchDescription(productName), executor)
                .exceptionally(ex -> {
                    System.err.println("Error fetching description for " + productName + ": " + ex.getMessage());
                    return "Нет данных";
                });
    }

    private CompletableFuture<Double> fetchRatingAsync(String productName) {
        return CompletableFuture.supplyAsync(() -> fetchRating(productName), executor)
                .exceptionally(ex -> {
                    System.err.println("Error fetching rating for " + productName + ": " + ex.getMessage());
                    return 0.0;
                });
    }

    private double fetchPrice(String productName) {
        simulateDelay("Price service");
        simulateRandomFailure("Price service");

        double basePrice = productName.equals("Ноутбук") ? 899.99 : 499.99;
        double variation = (random.nextDouble() - 0.5) * 200;
        return Math.round((basePrice + variation) * 100.0) / 100.0;
    }

    private String fetchDescription(String productName) {
        simulateDelay("Description service");
        simulateRandomFailure("Description service");

        return String.format("Высококачественный %s с отличными характеристиками. " +
                "Идеально подходит для работы и развлечений.", productName);
    }

    private double fetchRating(String productName) {
        simulateDelay("Rating service");
        simulateRandomFailure("Rating service");

        return Math.round((3.5 + random.nextDouble() * 1.5) * 10.0) / 10.0;
    }

    private void simulateDelay(String serviceName) {
        try {
            int delay = 1000 + random.nextInt(2000);
            System.out.printf("%s: processing %d ms...\n", serviceName, delay);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(serviceName + " interrupted", e);
        }
    }

    private void simulateRandomFailure(String serviceName) {
        if (random.nextDouble() < 0.2) {
            String[] errors = {
                    "Connection timeout",
                    "Service unavailable",
                    "Internal server error",
                    "Network error"
            };
            String errorMessage = errors[random.nextInt(errors.length)];
            throw new RuntimeException(serviceName + ": " + errorMessage);
        }
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
