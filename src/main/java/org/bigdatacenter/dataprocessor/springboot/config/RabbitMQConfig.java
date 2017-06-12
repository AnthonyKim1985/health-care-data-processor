package org.bigdatacenter.dataprocessor.springboot.config;

import org.bigdatacenter.dataprocessor.platform.rabbitmq.RabbitMQReceiverImpl;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by hyuk0 on 2017-06-02.
 */
@Configuration
public class RabbitMQConfig {
    public final static String queueName = "extraction-request-condition";

    @Bean
    public Queue queue() {
        return new Queue(queueName, false);
    }

    @Bean
    public TopicExchange exchange() {
        return new TopicExchange(queueName + "-exchange");
    }

    @Bean
    public Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue).to(exchange).with(queueName);
    }

    @Bean
    public SimpleMessageListenerContainer container(ConnectionFactory connectionFactory, MessageListenerAdapter listenerAdapter) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        container.setMessageListener(listenerAdapter);
        container.setMaxConcurrentConsumers(1);
        container.setReceiveTimeout(3000L);
        container.setRecoveryInterval(3000L);

        return container;
    }

    @Bean
    public MessageListenerAdapter listenerAdapter(RabbitMQReceiverImpl rabbitMQReceiverImpl) {
        return new MessageListenerAdapter(rabbitMQReceiverImpl, "runReceiver");
    }
}
