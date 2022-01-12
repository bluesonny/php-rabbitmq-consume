<?php

namespace app\components;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use Yii;

class RabbitMq
{
    private $connection;
    private $channel;
    public $work;

    /**
     * 基本配置信息.
     *
     * @var array
     */
    protected $_config = [
        'host' => '127.0.0.1',
        'port' => '5672',
        'vhost' => '/',
        'user' => 'guest',
        'password' => 'guest',
    ];

    public function __construct()
    {
        try {
            $config = Yii::$app->params['queue'] ?? $this->_config;
            $this->connection = new AMQPStreamConnection(
                $config['host'],
                $config['port'],
                $config['user'],
                $config['password'],
                $config['vhost'],
            );
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    private function init($exchange, $queue, $routing_key)
    {
        //require_once  APPPATH.'config/rabbitmq.php';
        //$this->connection = new AMQPConnection($this->config->item('host'), $this->config->item('port'), $this->config->item('user'), $this->config->item('pass'), $this->config->item('vhost'));
        $this->channel = $this->connection->channel();

        /*
            name: $queue
            passive: false  // don't check if an exchange with the same name exists
            durable: true // the queue will survive server restarts
            exclusive: false // the queue can be accessed in other channels
            auto_delete: false //the queue won't be deleted once the channel is closed.
        */
        $this->channel->queue_declare($queue, false, true, false, false);

        /*
            name: $exchange
            type: direct
            passive: false
            durable: true // the exchange will survive server restarts
            auto_delete: false //the exchange won't be deleted once the channel is closed.
        */
        $this->channel->exchange_declare($exchange, AMQPExchangeType::DIRECT, false, true, false);

        $this->channel->queue_bind($queue, $exchange, $routing_key);
    }

    /**
     * 发送消息.
     *
     * @param  [string] $queue        [队列名]
     * @param  [string] $send_message [发送消息]
     * @param string $exchange [交换机]
     *
     * @return null
     */
    public function send($queue, $send_message, $exchange = 'amq.direct')
    {
        $routing_key = $queue;
        try {
            $this->init($exchange, $queue, $routing_key);
            $message = new AMQPMessage($send_message, ['content_type' => 'text/plain', 'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
            $this->channel->basic_publish($message, $exchange, $routing_key);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    public function close()
    {
        $this->channel->close();
        $this->connection->close();
    }

    public function process($queue, $exchange = 'amq.direct')
    {
        $routing_key = $queue;
        $consumerTag = 'consumer';
        $this->init($exchange, $queue, $routing_key);

        //$this->channel->basic_qos(null, 10, null);

        /*
            queue: Queue from where to get the messages
            consumer_tag: Consumer identifier
            no_local: Don't receive messages published by this consumer.
            no_ack: Tells the server if the consumer will acknowledge the messages.
            exclusive: Request exclusive consumer access, meaning only this consumer can access the queue
            nowait:
            callback: A PHP Callback
        */
        $this->channel->basic_consume($queue, $consumerTag, false, false, false, false, [$this, 'process_message']);
        // Loop as long as the channel has callbacks registered
        //register_shutdown_function('close', $this->channel, $this->connection);

        while ($this->channel->is_consuming()) {
            $this->channel->wait();
        }
        $this->channel->close();
        $this->connection->close();
    }

    public function process_message($message)
    {
        $message_body = $message->body;
        $user_logic = $this->work;
        if (!is_callable($user_logic)) {
            throw new \Exception('需要实现自己的方法逻辑');
        }

        $is_ack = $user_logic($message_body);

        if ($is_ack) {
            $message->delivery_info['channel']->basic_ack($message->delivery_info['delivery_tag']);
        } else {
            // $message->delivery_info['channel']->basic_reject($message->delivery_info['delivery_tag'], true); //不停的循环
            //$message->delivery_info['channel']->basic_cancel($message->delivery_info['consumer_tag']); //守护进程退出
        }
    }
}
