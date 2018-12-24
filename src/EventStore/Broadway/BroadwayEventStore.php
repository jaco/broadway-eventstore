<?php
declare(strict_types=1);

namespace EventStore\Broadway;

use function array_merge;
use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use EventStore\EventStoreInterface;
use EventStore\Exception\StreamNotFoundException;
use EventStore\Exception\WrongExpectedVersionException;
use EventStore\StreamFeed\EntryWithEvent;
use EventStore\StreamFeed\Event;
use EventStore\WritableEvent;
use EventStore\WritableEventCollection;
use Broadway\Serializer\Serializer;


class BroadwayEventStore implements EventStore
{
    private $eventStore;
    private $payloadSerializer;
    private $metadataSerializer;

    public function __construct(EventStoreInterface $eventStore, Serializer $payloadSerializer, Serializer $metadataSerializer)
    {
        $this->eventStore = $eventStore;
        $this->payloadSerializer = $payloadSerializer;
        $this->metadataSerializer = $metadataSerializer;
    }

    /**
     * @inheritDoc
     */
    public function load($id): DomainEventStream
    {
        $iterator = $this
            ->eventStore
            ->forwardStreamFeedIterator($id);

        try {
            $iterator->rewind();
        } catch (StreamNotFoundException $e) {
            throw new EventStreamNotFoundException($e->getMessage());
        }

        $events = [];
        /** @var EntryWithEvent $entry */
        foreach($iterator as $entry) {
            $events[] = $this->buildDomainMessage($id, $entry->getEvent());
        }

        return new DomainEventStream($events);
    }


    public function loadFromPlayhead($id, int $playhead): DomainEventStream
    {
        throw new \Exception('Not implemented yet');
    }
    /**
     * @inheritDoc
     */
    public function append($id, DomainEventStream $eventStream)
    {
        $events = [];
        $playhead = null;

        foreach ($eventStream as $message) {
            $payload = $this->payloadSerializer->serialize($message->getPayload());

            if ($playhead === null) {
                $playhead = $message->getPlayhead();
            }

            $events[] = WritableEvent::newInstance(
                $payload['class'],
                array_merge(
                    $payload['payload'],
                    [
                        'broadway_recorded_on' => $message
                            ->getRecordedOn()
                            ->toString()
                    ]
                ),
                $this->metadataSerializer->serialize($message->getMetadata())
            );
        }

        if (empty($events)) {
            return;
        }

        try {
            $this
                ->eventStore
                ->writeToStream(
                    $id,
                    new WritableEventCollection($events),
                    $playhead - 1
                );
        } catch (WrongExpectedVersionException $e) {
            throw new DuplicatePlayheadException(new DomainEventStream($events), $e);
        }
    }


    private function buildDomainMessage($id, Event $event): DomainMessage
    {
        $data = $event->getData();
        $recordedOn = DateTime::fromString($data['broadway_recorded_on']);
        unset($data['broadway_recorded_on']);

        $object = $this->payloadSerializer->deserialize([
            'class' => $event->getType(),
            'payload' => $data
        ]);

        $metadata = $this->metadataSerializer->deserialize($event->getMetadata());

        return new DomainMessage(
            $id,
            $event->getVersion(),
            $metadata,
            $object,
            $recordedOn
        );
    }
}
