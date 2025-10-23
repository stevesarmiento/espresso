import { z } from 'zod';

// Hex regex pattern for program IDs (as stored in ClickHouse)
const HEX_REGEX = /^[0-9A-F]{64}$/i;

// Base58 regex pattern for Solana signatures
const BASE58_REGEX = /^[1-9A-HJ-NP-Za-km-z]{32,88}$/;

export const programIdSchema = z
  .string()
  .regex(HEX_REGEX, 'Invalid program ID format')
  .length(64, 'Program ID must be 64 hex characters');

export const signatureSchema = z
  .string()
  .regex(BASE58_REGEX, 'Invalid signature format')
  .min(87)
  .max(88);

export const slotSchema = z.coerce
  .number()
  .int()
  .positive('Slot must be positive')
  .max(Number.MAX_SAFE_INTEGER, 'Slot number too large');

export const timeRangeSchema = z.enum(['1h', '24h', '7d', '30d']);

export const paginationSchema = z.object({
  page: z.coerce.number().int().positive().default(1),
  limit: z.coerce.number().int().positive().max(100).default(50),
});

export const programSearchSchema = z.object({
  range: timeRangeSchema.default('24h'),
  limit: z.number().int().positive().max(100).optional(),
});

export const transactionSearchSchema = z.object({
  signature: signatureSchema.optional(),
  program_id: programIdSchema.optional(),
  start_slot: slotSchema.optional(),
  end_slot: slotSchema.optional(),
  limit: z.coerce.number().int().positive().max(50).default(20),
});

export function validateProgramId(id: string): boolean {
  try {
    programIdSchema.parse(id);
    return true;
  } catch {
    return false;
  }
}

export function validateSignature(sig: string): boolean {
  try {
    signatureSchema.parse(sig);
    return true;
  } catch {
    return false;
  }
}

export function validateSlot(slot: number): boolean {
  try {
    slotSchema.parse(slot);
    return true;
  } catch {
    return false;
  }
}

