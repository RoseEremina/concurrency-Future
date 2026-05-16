package com.eremin;

public class Main {
    public static void main(String[] args) {
        DataAggregator aggregator = new DataAggregator();

        System.out.println("Начинаем сбор данных для товара 'Ноутбук'...");

        long startTime = System.currentTimeMillis();

        ProductInfo product = aggregator.aggregateProductInfo("Ноутбук");

        long endTime = System.currentTimeMillis();

        System.out.println("\nРезультат:");
        System.out.println(product);
        System.out.printf("Время выполнения: %.2f секунд%n", (endTime - startTime) / 1000.0);

        System.out.println("\n" + "=".repeat(60));
        System.out.println("Собираем данные для товара 'Смартфон'...");

        long startTime2 = System.currentTimeMillis();
        ProductInfo product2 = aggregator.aggregateProductInfo("Смартфон");
        long endTime2 = System.currentTimeMillis();

        System.out.println("\nРезультат:");
        System.out.println(product2);
        System.out.printf("Время выполнения: %.2f секунд%n", (endTime2 - startTime2) / 1000.0);

        aggregator.shutdown();
    }
}
