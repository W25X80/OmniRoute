import { z } from "zod";
import { ArtifactStorageError } from "@/domain/persistence/artifactStore";

const schema = z
  .object({
    bucket: z
      .string()
      .regex(/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/)
      .refine(
        (name) =>
          !name.startsWith("xn--") &&
          !name.endsWith("--x-s3") &&
          !name.endsWith("--ol-s3") &&
          !name.endsWith("-s3alias")
      ),
    region: z
      .string()
      .max(63)
      .regex(/^[a-z]{2,8}(?:-[a-z0-9]+)+-\d+$/),
    prefix: z
      .string()
      .min(2)
      .max(256)
      .regex(/^[a-zA-Z0-9_-]+(?:\/[a-zA-Z0-9_-]+)*\/$/),
    expectedBucketOwner: z.string().regex(/^\d{12}$/),
    kmsKeyId: z
      .string()
      .regex(
        /^(?:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}|arn:aws[a-z-]*:kms:[a-z0-9-]+:\d{12}:key\/[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})$/
      )
      .optional(),
  })
  .strict();

export type S3ArtifactConfiguration = z.infer<typeof schema>;

export function parseS3ArtifactConfiguration(input: unknown): S3ArtifactConfiguration {
  const parsed = schema.safeParse(input);
  if (!parsed.success) throw new ArtifactStorageError("configuration");
  return parsed.data;
}

export function readS3ArtifactConfiguration(
  env: NodeJS.ProcessEnv = process.env
): S3ArtifactConfiguration {
  return parseS3ArtifactConfiguration({
    bucket: env.OMNIROUTE_ARTIFACT_S3_BUCKET,
    region: env.AWS_REGION,
    prefix: env.OMNIROUTE_ARTIFACT_S3_PREFIX,
    expectedBucketOwner: env.OMNIROUTE_ARTIFACT_S3_EXPECTED_BUCKET_OWNER,
    ...(env.OMNIROUTE_ARTIFACT_S3_KMS_KEY_ID
      ? { kmsKeyId: env.OMNIROUTE_ARTIFACT_S3_KMS_KEY_ID }
      : {}),
  });
}
