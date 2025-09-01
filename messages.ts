import { badRequest, post, objectSpreadable } from '@riddance/service/http'
import type { Json, Context } from '@riddance/service/context'

const MESSAGE_TYPES = {
    ROCKET_LAUNCHED: 'RocketLaunched',
    ROCKET_SPEED_INCREASED: 'RocketSpeedIncreased',
    ROCKET_SPEED_DECREASED: 'RocketSpeedDecreased',
    ROCKET_EXPLODED: 'RocketExploded',
    ROCKET_MISSION_CHANGED: 'RocketMissionChanged',
} as const

type MessageType = (typeof MESSAGE_TYPES)[keyof typeof MESSAGE_TYPES]

// Base metadata type
type BaseRocketMessageMetadata = {
    channel: string
    messageNumber: number
    messageTime: string
}

// Discriminated union types for each message
type RocketLaunchedMessage = {
    metadata: BaseRocketMessageMetadata & { messageType: typeof MESSAGE_TYPES.ROCKET_LAUNCHED }
    message: {
        type: string
        launchSpeed: number
        mission: string
    }
}

type RocketSpeedIncreasedMessage = {
    metadata: BaseRocketMessageMetadata & {
        messageType: typeof MESSAGE_TYPES.ROCKET_SPEED_INCREASED
    }
    message: {
        by: number
    }
}

type RocketSpeedDecreasedMessage = {
    metadata: BaseRocketMessageMetadata & {
        messageType: typeof MESSAGE_TYPES.ROCKET_SPEED_DECREASED
    }
    message: {
        by: number
    }
}

type RocketExplodedMessage = {
    metadata: BaseRocketMessageMetadata & { messageType: typeof MESSAGE_TYPES.ROCKET_EXPLODED }
    message: {
        reason: string
    }
}

type RocketMissionChangedMessage = {
    metadata: BaseRocketMessageMetadata & {
        messageType: typeof MESSAGE_TYPES.ROCKET_MISSION_CHANGED
    }
    message: {
        newMission: string
    }
}

// Discriminated union of all message types
type RocketMessage =
    | RocketLaunchedMessage
    | RocketSpeedIncreasedMessage
    | RocketSpeedDecreasedMessage
    | RocketExplodedMessage
    | RocketMissionChangedMessage

// Type guards for better type safety
function isValidJson(value: unknown): value is Json {
    return value !== undefined
}

function isMessageType(value: string): value is MessageType {
    return Object.values(MESSAGE_TYPES).includes(value as MessageType)
}

function validateBaseMessage(data: unknown) {
    if (!data || typeof data !== 'object') {
        throw badRequest('Invalid message format')
    }

    const message = objectSpreadable(isValidJson(data) ? data : {})

    if (!message.metadata || typeof message.metadata !== 'object') {
        throw badRequest('Missing or invalid metadata')
    }
    if (!message.message || typeof message.message !== 'object') {
        throw badRequest('Missing or invalid message payload')
    }

    const metadata = objectSpreadable(isValidJson(message.metadata) ? message.metadata : {})
    const payload = objectSpreadable(isValidJson(message.message) ? message.message : {})

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
    if (!isMessageType(metadata.messageType)) {
        throw badRequest('Invalid messageType')
    }

    return {
        metadata: {
            channel: metadata.channel,
            messageNumber: metadata.messageNumber,
            messageTime: metadata.messageTime,
            messageType: metadata.messageType,
        },
        payload,
    }
}

function validateMessage(data: unknown): RocketMessage {
    const { metadata, payload } = validateBaseMessage(data)

    switch (metadata.messageType) {
        case MESSAGE_TYPES.ROCKET_LAUNCHED:
            if (typeof payload.type !== 'string') {
                throw badRequest('Missing or invalid rocket type')
            }
            if (typeof payload.launchSpeed !== 'number') {
                throw badRequest('Missing or invalid launchSpeed')
            }
            if (typeof payload.mission !== 'string') {
                throw badRequest('Missing or invalid mission')
            }
            return {
                metadata: {
                    ...metadata,
                    messageType: MESSAGE_TYPES.ROCKET_LAUNCHED,
                },
                message: {
                    type: payload.type,
                    launchSpeed: payload.launchSpeed,
                    mission: payload.mission,
                },
            } satisfies RocketLaunchedMessage

        case MESSAGE_TYPES.ROCKET_SPEED_INCREASED:
            if (typeof payload.by !== 'number') {
                throw badRequest('Missing or invalid speed change amount')
            }
            return {
                metadata: {
                    ...metadata,
                    messageType: MESSAGE_TYPES.ROCKET_SPEED_INCREASED,
                },
                message: { by: payload.by },
            } satisfies RocketSpeedIncreasedMessage

        case MESSAGE_TYPES.ROCKET_SPEED_DECREASED:
            if (typeof payload.by !== 'number') {
                throw badRequest('Missing or invalid speed change amount')
            }
            return {
                metadata: {
                    ...metadata,
                    messageType: MESSAGE_TYPES.ROCKET_SPEED_DECREASED,
                },
                message: { by: payload.by },
            } satisfies RocketSpeedDecreasedMessage

        case MESSAGE_TYPES.ROCKET_EXPLODED:
            if (typeof payload.reason !== 'string') {
                throw badRequest('Missing or invalid explosion reason')
            }
            return {
                metadata: {
                    ...metadata,
                    messageType: MESSAGE_TYPES.ROCKET_EXPLODED,
                },
                message: { reason: payload.reason },
            } satisfies RocketExplodedMessage

        case MESSAGE_TYPES.ROCKET_MISSION_CHANGED:
            if (typeof payload.newMission !== 'string') {
                throw badRequest('Missing or invalid new mission')
            }
            return {
                metadata: {
                    ...metadata,
                    messageType: MESSAGE_TYPES.ROCKET_MISSION_CHANGED,
                },
                message: { newMission: payload.newMission },
            } satisfies RocketMissionChangedMessage

        default:
            // TypeScript ensures exhaustive checking
            throw badRequest('Unhandled message type')
    }
}

// Type predicates for proper type narrowing
function isRocketLaunchedMessage(message: RocketMessage): message is RocketLaunchedMessage {
    return message.metadata.messageType === MESSAGE_TYPES.ROCKET_LAUNCHED
}

function isRocketSpeedIncreasedMessage(
    message: RocketMessage,
): message is RocketSpeedIncreasedMessage {
    return message.metadata.messageType === MESSAGE_TYPES.ROCKET_SPEED_INCREASED
}

function isRocketSpeedDecreasedMessage(
    message: RocketMessage,
): message is RocketSpeedDecreasedMessage {
    return message.metadata.messageType === MESSAGE_TYPES.ROCKET_SPEED_DECREASED
}

function isRocketExplodedMessage(message: RocketMessage): message is RocketExplodedMessage {
    return message.metadata.messageType === MESSAGE_TYPES.ROCKET_EXPLODED
}

function isRocketMissionChangedMessage(
    message: RocketMessage,
): message is RocketMissionChangedMessage {
    return message.metadata.messageType === MESSAGE_TYPES.ROCKET_MISSION_CHANGED
}

async function emitRocketEvent(context: Context, message: RocketMessage): Promise<void> {
    const eventTopic = 'rocket'
    const eventSubject = message.metadata.channel

    if (isRocketLaunchedMessage(message)) {
        await context.emit(eventTopic, 'launched', eventSubject, {
            type: message.message.type,
            launchSpeed: message.message.launchSpeed,
            mission: message.message.mission,
        })
    } else if (isRocketSpeedIncreasedMessage(message)) {
        await context.emit(eventTopic, 'speed-increased', eventSubject, {
            by: message.message.by,
        })
    } else if (isRocketSpeedDecreasedMessage(message)) {
        await context.emit(eventTopic, 'speed-decreased', eventSubject, {
            by: message.message.by,
        })
    } else if (isRocketExplodedMessage(message)) {
        await context.emit(eventTopic, 'exploded', eventSubject, {
            reason: message.message.reason,
        })
    } else if (isRocketMissionChangedMessage(message)) {
        await context.emit(eventTopic, 'mission-changed', eventSubject, {
            newMission: message.message.newMission,
        })
    } else {
        // This should never happen due to validation
        throw new Error('Unhandled message type')
    }
}

post('messages', async (context, request) => {
    const rocketMessage = validateMessage(request.body)
    await emitRocketEvent(context, rocketMessage)
    return { status: 200 }
})
