import { badRequest, post, objectSpreadable } from '@riddance/service/http'
import type { Json, Context } from '@riddance/service/context'

const MESSAGE_TYPES = {
    ROCKET_LAUNCHED: 'RocketLaunched',
    ROCKET_SPEED_INCREASED: 'RocketSpeedIncreased',
    ROCKET_SPEED_DECREASED: 'RocketSpeedDecreased',
    ROCKET_EXPLODED: 'RocketExploded',
    ROCKET_MISSION_CHANGED: 'RocketMissionChanged',
} as const

const VALID_MESSAGE_TYPES = Object.values(MESSAGE_TYPES)

type MessageType = (typeof MESSAGE_TYPES)[keyof typeof MESSAGE_TYPES]

type RocketMessageMetadata = {
    channel: string
    messageNumber: number
    messageTime: string
    messageType: MessageType
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

function validateMessage(data: unknown): RocketMessage {
    if (!data || typeof data !== 'object') {
        throw badRequest('Invalid message format')
    }

    const message = objectSpreadable(data as Json)

    if (!message.metadata || typeof message.metadata !== 'object') {
        throw badRequest('Missing or invalid metadata')
    }
    if (!message.message || typeof message.message !== 'object') {
        throw badRequest('Missing or invalid message payload')
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
    if (!VALID_MESSAGE_TYPES.includes(metadata.messageType as MessageType)) {
        throw badRequest('Invalid messageType')
    }

    return {
        metadata: metadata as RocketMessageMetadata,
        message: objectSpreadable(message.message as Json) as RocketMessagePayload,
    }
}

async function emitRocketEvent(context: Context, message: RocketMessage): Promise<void> {
    const { channel, messageType } = message.metadata
    const eventTopic = 'rocket'
    const eventSubject = channel

    switch (messageType) {
        case MESSAGE_TYPES.ROCKET_LAUNCHED: {
            const payload = message.message as RocketLaunchedMessage
            await context.emit(eventTopic, 'launched', eventSubject, {
                type: payload.type,
                launchSpeed: payload.launchSpeed,
                mission: payload.mission,
            })
            break
        }

        case MESSAGE_TYPES.ROCKET_SPEED_INCREASED: {
            const payload = message.message as RocketSpeedChangedMessage
            await context.emit(eventTopic, 'speed-increased', eventSubject, {
                by: payload.by,
            })
            break
        }

        case MESSAGE_TYPES.ROCKET_SPEED_DECREASED: {
            const payload = message.message as RocketSpeedChangedMessage
            await context.emit(eventTopic, 'speed-decreased', eventSubject, {
                by: payload.by,
            })
            break
        }

        case MESSAGE_TYPES.ROCKET_EXPLODED: {
            const payload = message.message as RocketExplodedMessage
            await context.emit(eventTopic, 'exploded', eventSubject, {
                reason: payload.reason,
            })
            break
        }

        case MESSAGE_TYPES.ROCKET_MISSION_CHANGED: {
            const payload = message.message as RocketMissionChangedMessage
            await context.emit(eventTopic, 'mission-changed', eventSubject, {
                newMission: payload.newMission,
            })
            break
        }
    }
}

post('messages', async (context, request) => {
    const rocketMessage = validateMessage(request.body)
    await emitRocketEvent(context, rocketMessage)
    return { status: 200 }
})
