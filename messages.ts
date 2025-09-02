import type { Json } from '@riddance/service/context'
import { badRequest, objectSpreadable, post } from '@riddance/service/http'

post('messages', async (context, request) => {
    const data = validateMessage(request.body)
    await context.emit('rocket', topicMap[data.type], data.metadata.channel, {
        meta: {
            timestamp: data.metadata.messageTime,
            sequence: data.metadata.messageNumber,
        },
        ...data.message,
    })
})

type RocketData =
    | {
          metadata: Metadata
          type: 'RocketLaunched'
          message: {
              type: string
              launchSpeed: number
              mission: string
          }
      }
    | {
          metadata: Metadata
          type: 'RocketSpeedDecreased'
          message: {
              by: number
          }
      }
    | {
          metadata: Metadata
          type: 'RocketSpeedIncreased'
          message: {
              by: number
          }
      }
    | {
          metadata: Metadata
          type: 'RocketExploded'
          message: {
              reason: string
          }
      }
    | {
          metadata: Metadata
          type: 'RocketMissionChanged'
          message: {
              newMission: string
          }
      }

type Metadata = {
    channel: string
    messageNumber: number
    messageTime: string
}

function validateMessage(data: Json | undefined): RocketData {
    const { metadata, type, message } = validateBaseMessage(data)

    switch (type) {
        case 'RocketLaunched':
            if (typeof message.type !== 'string') {
                throw badRequest('Missing or invalid rocket type')
            }
            if (typeof message.launchSpeed !== 'number' || message.launchSpeed < 0) {
                throw badRequest('Missing or invalid launchSpeed')
            }
            if (typeof message.mission !== 'string') {
                throw badRequest('Missing or invalid mission')
            }
            return {
                metadata,
                type: 'RocketLaunched',
                message: {
                    type: message.type,
                    launchSpeed: message.launchSpeed,
                    mission: message.mission,
                },
            }
        case 'RocketSpeedIncreased':
        case 'RocketSpeedDecreased':
            if (typeof message.by !== 'number' || message.by < 0) {
                throw badRequest('Missing or invalid speed change amount')
            }
            return {
                metadata,
                type,
                message: { by: message.by },
            }
        case 'RocketExploded':
            if (typeof message.reason !== 'string') {
                throw badRequest('Missing or invalid explosion reason')
            }
            return {
                metadata,
                type: 'RocketExploded',
                message: { reason: message.reason },
            }
        case 'RocketMissionChanged':
            if (typeof message.newMission !== 'string') {
                throw badRequest('Missing or invalid new mission')
            }
            return {
                metadata,
                type: 'RocketMissionChanged',
                message: { newMission: message.newMission },
            }
    }
}

const topicMap = {
    RocketLaunched: 'launched',
    RocketSpeedIncreased: 'speed-increased',
    RocketSpeedDecreased: 'speed-decreased',
    RocketExploded: 'exploded',
    RocketMissionChanged: 'mission-changed',
} as const

const validTypes = [
    'RocketLaunched',
    'RocketSpeedIncreased',
    'RocketSpeedDecreased',
    'RocketExploded',
    'RocketMissionChanged',
] as const

type MessageType = (typeof validTypes)[number]

function isMessageType(value: unknown): value is MessageType {
    return validTypes.includes(value as MessageType)
}

function validateBaseMessage(data: Json | undefined) {
    const { metadata, message } = objectSpreadable(data)
    const { channel, messageNumber, messageTime, messageType } = objectSpreadable(metadata)

    if (typeof channel !== 'string') {
        throw badRequest('Missing or invalid channel')
    }
    if (typeof messageNumber !== 'number') {
        throw badRequest('Missing or invalid messageNumber')
    }
    if (typeof messageTime !== 'string') {
        throw badRequest('Missing or invalid messageTime')
    }
    if (!isMessageType(messageType)) {
        throw badRequest('Invalid messageType')
    }

    return {
        metadata: {
            channel,
            messageNumber,
            messageTime,
        },
        type: messageType,
        message: objectSpreadable(message),
    }
}
