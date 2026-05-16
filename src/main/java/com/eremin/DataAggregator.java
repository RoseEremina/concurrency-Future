package com.eremin;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class DataAggregator {
    private static final Random random = new Random();
    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    private CompletableFuture<Double> fetchPrice(String productName) {
        return CompletableFuture.supplyAsync(() -> {
            simulateDelay();
            simulateError(0.2);
            if ("Ноутбук".equals(productName)) return 899.99;
            if ("Смартфон".equals(productName)) return 699.50;
            return 499.99;
        }, executor);
    }

    private CompletableFuture<String> fetchDescription(String productName) {
        return CompletableFuture.supplyAsync(() -> {
            simulateDelay();
            simulateError(0.2);
            if ("Ноутбук".equals(productName)) return "Мощный игровой ноутбук с 16GB RAM и SSD 512GB";
            if ("Смартфон".equals(productName)) return "Смартфон с отличной камерой и большим экраном";
            return "Качественный товар";
        }, executor);
    }

    private CompletableFuture<Double> fetchRating(String productName) {
        return CompletableFuture.supplyAsync(() -> {
            simulateDelay();
            simulateError(0.2);
            if ("Ноутбук".equals(productName)) return 4.7;
            if ("Смартфон".equals(productName)) return 4.5;
            return 4.0;
        }, executor);
    }

    private void simulateDelay() {
        try {
            int delay = 1000 + random.nextInt(2001);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void simulateError(double probability) {
        if (random.nextDouble() < probability) {
            throw new RuntimeException("Сервис временно недоступен");
        }
    }

    public ProductInfo aggregateProductInfo(String productName) {
        CompletableFuture<Double> priceFuture = fetchPrice(productName)
                .exceptionally(throwable -> {
                    System.err.println("Ошибка получения цены для '" + productName + "': " + throwable.getMessage() + ". Используем значение по умолчанию: 0.0");
                    return 0.0;
                });

        CompletableFuture<String> descriptionFuture = fetchDescription(productName)
                .exceptionally(throwable -> {
                    System.err.println("Ошибка получения описания для '" + productName + "': " + throwable.getMessage() + ". Используем значение по умолчанию: 'Нет данных'");
                    return "Нет данных";
                });

        CompletableFuture<Double> ratingFuture = fetchRating(productName)
                .exceptionally(throwable -> {
                    System.err.println("Ошибка получения рейтинга для '" + productName + "': " + throwable.getMessage() + ". Используем значение по умолчанию: 0.0");
                    return 0.0;
                });

        CompletableFuture<ProductInfo> productFuture = priceFuture
                .thenCombine(descriptionFuture, (price, description) ->
                        new Object[]{price, description})
                .thenCombine(ratingFuture, (combined, rating) ->
                        new ProductInfo(productName, (Double) combined[0], (String) combined[1], rating));

        return productFuture.join();
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
