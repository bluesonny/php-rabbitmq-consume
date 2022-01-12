<?php

namespace Api\PhpUtils\RabbitMq;

use Api\PhpUtils\Log\FileLog;

/**
 * RabiitMq工具类
 * 
 * amqp.so verion  1.10.2
 */


class DelayRabbitMq
{
    private $conn;
    private $channel;
    private $exchange;
    private $queue;
    private $config;
    public  $work;
    public $configIndex;


    public function connection()
    {
        try {
            
            $this->conn = new \AMQPConnection(
                $this->config
            );
            $this->conn->connect();
            if ($this->conn->isConnected()) {
                FileLog::info('RabbitMqUtil __construct-info:' . '连接成功');
            } else {
                FileLog::error('RabbitMqUtil __construct-error:' . '连接失败-' . json_encode($this->config));
            }
        } catch (\Exception $e) {
            FileLog::error('RabbitMqUtil __construct-Exception:' . $e->getMessage());
        }
    }

    private function getConfig($configIndex, $flags)
    {
        $arr =  \Yaconf::get('rabbitmq');
        $this->config = $arr['common'][$configIndex]['proxy'];
        if ($flags != 'produce') {
            $this->config = $arr['common'][$configIndex]['consume'];
        }
    }
    /**
     * 初始化
     * @param  string $exchange    [交换机]
     * @param  string $queue       [队列名]
     * @param  string $routing_key [路由]
     * @return null
     */
    private function init($exchange, $queue, $routingKey, $flags = 'produce')
    {
        $this->getConfig($this->configIndex, $flags);
        $this->connection($flags);
        try {
            $this->channel = new \AMQPChannel($this->conn);
            /*
            name: $exchange
            type: x-delayed-message
            passive: false
            durable: true // the exchange will survive server restarts
            auto_delete: false //the exchange won't be deleted once the channel is closed.
        */
            $this->exchange = new \AMQPExchange($this->channel);
            $this->exchange->setName($exchange);
            $this->exchange->setType('x-delayed-message');
            $this->exchange->setArgument('x-delayed-type', 'direct');
            if ($flags == 'produce') {
                $this->exchange->setFlags(AMQP_DURABLE);
            } else {
                $this->exchange->setFlags(AMQP_PASSIVE); //声明一个已存在的交换器的，如果不存在将抛出异常，这个一般用在consume端
            }
            $this->exchange->declareExchange();

            /*
            name: $queue
            passive: false  // don't check if an exchange with the same name exists
            durable: true // the queue will survive server restarts
            exclusive: false // the queue can be accessed in other channels
            auto_delete: false //the queue won't be deleted once the channel is closed.
        */
            $this->queue = new \AMQPQueue($this->channel);
            $this->queue->setName($queue);
            $this->queue->setFlags(AMQP_DURABLE);
            $this->queue->declareQueue();
            $this->queue->bind($exchange, $routingKey);
        } catch (\Exception $e) {
            FileLog::error('RabbitMqUtil __init-Exception' . $e->getMessage());
        }
    }

    /**
     * 发送消息
     * @param  string $queue        [队列名]
     * @param  string $sendMessage [发送消息]
     * @param  int    $minute       [延迟时间 - 分]
     * @param  string $exchange     [交换机]
     * @return null
     */
    public function produce($queue, $sendMessage, $minute, $exchange = 'x-delayed-message')
    {
        $routing_key = $queue;
        try {
            $this->init($exchange, $queue, $routing_key);
            $this->exchange->publish(
                $sendMessage,
                $routing_key,
                AMQP_MANDATORY, //mandatory标志告诉服务器至少将该消息route到一个队列中，否则将消息返还给生产者
                array(
                    'headers' => [
                        'x-delay' => 1000 * 60 * $minute
                    ]
                )
            );

            $this->conn->disconnect();
        } catch (\Exception $e) {
            FileLog::error('RabbitMqUtil __produce-Exception:' . $e->getMessage());
        }
    }


    public function process($queue, $exchange = 'x-delayed-message')
    {
        $routing_key = $queue;
        $this->init($exchange, $queue, $routing_key, 'process');
        try {
            $this->queue->consume([$this, 'processMessage']);
        } catch (\Exception $e) {
            FileLog::error('RabbitMqUtil __process-Exception:' . $e->getMessage());
        }

        $this->conn->disconnect();
    }

    public function processMessage($envelope)
    {
        $message_body = $envelope->getBody();
        $userLogic = $this->work;
        if (!is_callable($userLogic)) {
            throw new \Exception("需要实现自己的方法逻辑");
        }
        $is_ack = $userLogic($message_body);
        if ($is_ack) {
            $this->queue->ack($envelope->getDeliveryTag());
        } else {
            //$this->queue->nack($envelope->getDeliveryTag());
            //无应答 会存放在unacked状态下重连会放回ready，如果发送unack 状态 后会直接放回 ready 
        }
    }
}
