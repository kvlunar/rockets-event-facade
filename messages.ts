import { badRequest, post, objectSpreadable } from '@riddance/service/http'
import type { Json } from '@riddance/service/context'

type RocketMessageMetadata = {
    channel: string
    messageNumber: number
    messageTime: string
    messageType:
        | 'RocketLaunched'
        | 'RocketSpeedIncreased'
        | 'RocketSpeedDecreased'
        | 'RocketExploded'
        | 'RocketMissionChanged'
}

type RocketLaunchedMessage = {
    type: string
    launchSpeed: number
    mission: string
}

type RocketSpeedChangedMessage = {
    by: number
}

type RocketExplodedMessage = {
    reason: string
}

type RocketMissionChangedMessage = {
    newMission: string
}

type RocketMessagePayload =
    | RocketLaunchedMessage
    | RocketSpeedChangedMessage
    | RocketExplodedMessage
    | RocketMissionChangedMessage

type RocketMessage = {
    metadata: RocketMessageMetadata
    message: RocketMessagePayload
}

const messageStorage = new Map<string, Set<number>>()

function isDuplicate(channel: string, messageNumber: number): boolean {
    const channelMessages = messageStorage.get(channel)
    if (!channelMessages) {
        return false
    }
    return channelMessages.has(messageNumber)
}

function recordMessage(channel: string, messageNumber: number): void {
    const channelMessages = messageStorage.get(channel) ?? new Set<number>()
    channelMessages.add(messageNumber)
    messageStorage.set(channel, channelMessages)
}

function validateMessage(data: unknown): RocketMessage {
    if (!data || typeof data !== 'object') {
        throw badRequest('Invalid message format')
    }

    const message = objectSpreadable(data as Json)

    if (!message.metadata || typeof message.metadata !== 'object') {
        throw badRequest('Missing or invalid metadata')
    }

    const metadata = objectSpreadable(message.metadata as Json)

    if (!metadata.channel || typeof metadata.channel !== 'string') {
        throw badRequest('Missing or invalid channel')
    }

    if (typeof metadata.messageNumber !== 'number') {
        throw badRequest('Missing or invalid messageNumber')
    }

    if (!metadata.messageTime || typeof metadata.messageTime !== 'string') {
        throw badRequest('Missing or invalid messageTime')
    }

    if (!metadata.messageType || typeof metadata.messageType !== 'string') {
        throw badRequest('Missing or invalid messageType')
    }

    const validMessageTypes = [
        'RocketLaunched',
        'RocketSpeedIncreased',
        'RocketSpeedDecreased',
        'RocketExploded',
        'RocketMissionChanged',
    ]
    if (!validMessageTypes.includes(metadata.messageType)) {
        throw badRequest('Invalid messageType')
    }

    if (!message.message || typeof message.message !== 'object') {
        throw badRequest('Missing or invalid message payload')
    }

    return {
        metadata: metadata as RocketMessageMetadata,
        message: objectSpreadable(message.message as Json) as RocketMessagePayload,
    }
}

post('messages', async (context, request) => {
    const rocketMessage = validateMessage(request.body)

    const { channel, messageNumber, messageType } = rocketMessage.metadata

    if (isDuplicate(channel, messageNumber)) {
        return { status: 200 }
    }

    recordMessage(channel, messageNumber)

    const eventTopic = 'rocket'
    const eventSubject = channel

    switch (messageType) {
        case 'RocketLaunched': {
            const payload = rocketMessage.message as RocketLaunchedMessage
            await context.emit(eventTopic, 'launched', eventSubject, {
                type: payload.type,
                launchSpeed: payload.launchSpeed,
                mission: payload.mission,
            })
            break
        }

        case 'RocketSpeedIncreased': {
            const payload = rocketMessage.message as RocketSpeedChangedMessage
            await context.emit(eventTopic, 'speed-increased', eventSubject, {
                by: payload.by,
            })
            break
        }

        case 'RocketSpeedDecreased': {
            const payload = rocketMessage.message as RocketSpeedChangedMessage
            await context.emit(eventTopic, 'speed-decreased', eventSubject, {
                by: payload.by,
            })
            break
        }

        case 'RocketExploded': {
            const payload = rocketMessage.message as RocketExplodedMessage
            await context.emit(eventTopic, 'exploded', eventSubject, {
                reason: payload.reason,
            })
            break
        }

        case 'RocketMissionChanged': {
            const payload = rocketMessage.message as RocketMissionChangedMessage
            await context.emit(eventTopic, 'mission-changed', eventSubject, {
                newMission: payload.newMission,
            })
            break
        }
    }

    return { status: 200 }
})
