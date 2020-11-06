package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.LogUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试ETL相关简单应用
 * <br/>
 * 参照文本: <a href='https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/learn-flink/etl.html'>数据管道 & ETL</a>
 */
public class CheckoutCost {

	public static void main(final String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(8);

		// 定价流, 输入定价
		DataStream<Price> priceStrList
			= env.fromElements("p1,1", "p2,2", "p3,3")
			.flatMap(new FlatMapFunction<String, Price>() {
				@Override
				public void flatMap(String value, Collector<Price> out) {
					String[] arr = value.split(",");
					if (arr.length == 2) {
						try {
							out.collect(new Price(arr[0], Double.valueOf(arr[1])));
						} catch (Exception e) {
							LogUtil.info("invalid price str:%s", value);
						}
					}
				}
			})
			.keyBy(new KeySelector<Price, String>() {
				@Override
				public String getKey(Price value) {
					return value.productId;
				}
			});

		// 订单流, 输入订单数量
		DataStream<Order> orderStrList
			= env.socketTextStream("127.0.0.1", 8080)
			.flatMap(new FlatMapFunction<String, Order>() {
				@Override
				public void flatMap(String value, Collector<Order> out) {
					String[] arr = value.split(",");
					if (arr.length == 2) {
						try {
							out.collect(new Order(arr[0], Integer.valueOf(arr[1])));
						} catch (Exception e) {
							LogUtil.info("invalid order str:%s", value);
						}
					}
				}
			})
			.keyBy(new KeySelector<Order, String>() {
				@Override
				public String getKey(Order value) {
					return value.productId;
				}
			});

		priceStrList
			.connect(orderStrList)
			.flatMap(new CoFlatMapFunction<Price, Order, Double>() {
				private Map<String, Double> productId2Price = new HashMap<>();
				@Override
				public void flatMap1(Price value, Collector<Double> out) {
					productId2Price.put(value.getProductId(), value.getPrice());
				}

				@Override
				public void flatMap2(Order value, Collector<Double> out) {
					if (productId2Price.containsKey(value.getProductId())) {
						Double price = productId2Price.get(value.getProductId());
						out.collect(value.getCount() * price);
					} else {
						LogUtil.info("Unknown product:%s", value.getProductId());
					}
				}
			})
			.print();
		env.execute();
	}

	public static class Price {
		private String productId;
		private Double price;

		public Price() {
		}

		public Price(String productId, Double price) {
			this.productId = productId;
			this.price = price;
		}

		public String getProductId() {
			return productId;
		}

		public void setProductId(String productId) {
			this.productId = productId;
		}

		public Double getPrice() {
			return price;
		}

		public void setPrice(Double price) {
			this.price = price;
		}

		@Override
		public String toString() {
			return "Price{" +
				"productId='" + productId + '\'' +
				", price=" + price +
				'}';
		}
	}

	public static class Order {
		private String productId;
		private Integer count;

		public Order() {
		}

		public Order(String productId, Integer count) {
			this.productId = productId;
			this.count = count;
		}

		public String getProductId() {
			return productId;
		}

		public void setProductId(String productId) {
			this.productId = productId;
		}

		public Integer getCount() {
			return count;
		}

		public void setCount(Integer count) {
			this.count = count;
		}
	}
}
