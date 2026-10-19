import { scopedCredentialSelector } from "@omniroute/open-sse/services/scopedCredentialSelection.ts";
import {
  getProviderCredentials as selectCredentials,
  getProviderCredentialsWithQuotaPreflight as selectWithPreflight,
} from "./auth";

export const getProviderCredentials = scopedCredentialSelector(selectCredentials);
export const getProviderCredentialsWithQuotaPreflight =
  scopedCredentialSelector(selectWithPreflight);
