<?php

namespace tonisormisson\log\target\cloudwatch;

use Aws\CloudWatchLogs\CloudWatchLogsClient;
use Exception;
use Yiisoft\Log\Target;

class CloudWatchTarget extends Target
{
    private CloudWatchLogsClient $client;
    private string $logGroupName;
    private string $logStreamName;

    public function __construct(
        CloudWatchLogsClient $client,
        string $logGroupName,
        string $logStreamName)
    {
        $this->client = $client;
        $this->logGroupName = $logGroupName;
        $this->logStreamName = $logStreamName;
        parent::__construct();

    }

    public function export() : void
    {
        $logEvents = [];
        $defaultLogTime = round(microtime(true) * 1000);

        foreach ($this->getMessages() as $key => $message) {
            $time = $message->context('time', $defaultLogTime);

            $data = [
                'message' => $message->message(),
            ];
            foreach ($message->context() as $key => $value) {
                $data[$key] = $value;
            }

            $logEvents[] = [
                'timestamp' => $time,
                'message' => json_encode($data),
            ];
        }

        try {
            $nextSequenceToken =  $this->fetchSequenceTokenFromSource();
            $logConfig = [
                'logGroupName' => $this->logGroupName,
                'logStreamName' => $this->logStreamName,
                'logEvents' =>  $logEvents,
            ];
            if(!empty($nextSequenceToken)){
                $logConfig['sequenceToken'] = $nextSequenceToken;
            }
            $this->client->putLogEvents($logConfig);

        } catch (Exception $e) {

        }

    }

    private function fetchSequenceTokenFromSource() : string
    {
        $streamsResult = $this->client->describeLogStreams(['logGroupName' => $this->logGroupName]);
        $streams = $streamsResult->get('logStreams');
        foreach ($streams as $stream) {
            if($stream['logStreamName'] === $this->logStreamName) {
                if(!isset($stream['uploadSequenceToken'])) {
                    return '';
                }
                return $stream['uploadSequenceToken'];
            }
        }
        throw new Exception("No result response");
    }

}