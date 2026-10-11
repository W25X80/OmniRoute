import * as sqlite from "./domainState";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { PostgresDomainStateRepository } from "./postgres/domainStateRepository";

export async function saveFallbackChainAsync(...args: Parameters<typeof sqlite.saveFallbackChain>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveFallbackChain(...args)
    : sqlite.saveFallbackChain(...args);
}

export async function loadFallbackChainAsync(...args: Parameters<typeof sqlite.loadFallbackChain>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadFallbackChain(...args)
    : sqlite.loadFallbackChain(...args);
}

export async function loadAllFallbackChainsAsync(
  ...args: Parameters<typeof sqlite.loadAllFallbackChains>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadAllFallbackChains(...args)
    : sqlite.loadAllFallbackChains(...args);
}

export async function deleteFallbackChainAsync(
  ...args: Parameters<typeof sqlite.deleteFallbackChain>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteFallbackChain(...args)
    : sqlite.deleteFallbackChain(...args);
}

export async function deleteAllFallbackChainsAsync(
  ...args: Parameters<typeof sqlite.deleteAllFallbackChains>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteAllFallbackChains(...args)
    : sqlite.deleteAllFallbackChains(...args);
}

export async function saveBudgetAsync(...args: Parameters<typeof sqlite.saveBudget>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveBudget(...args)
    : sqlite.saveBudget(...args);
}

export async function loadBudgetAsync(...args: Parameters<typeof sqlite.loadBudget>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadBudget(...args)
    : sqlite.loadBudget(...args);
}

export async function loadAllBudgetsAsync(...args: Parameters<typeof sqlite.loadAllBudgets>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadAllBudgets(...args)
    : sqlite.loadAllBudgets(...args);
}

export async function saveBudgetResetLogAsync(
  ...args: Parameters<typeof sqlite.saveBudgetResetLog>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveBudgetResetLog(...args)
    : sqlite.saveBudgetResetLog(...args);
}

export async function loadBudgetResetLogsAsync(
  ...args: Parameters<typeof sqlite.loadBudgetResetLogs>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadBudgetResetLogs(...args)
    : sqlite.loadBudgetResetLogs(...args);
}

export async function deleteBudgetAsync(...args: Parameters<typeof sqlite.deleteBudget>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteBudget(...args)
    : sqlite.deleteBudget(...args);
}

export async function saveCostEntryAsync(...args: Parameters<typeof sqlite.saveCostEntry>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveCostEntry(...args)
    : sqlite.saveCostEntry(...args);
}

export async function batchSaveCostEntriesAsync(
  ...args: Parameters<typeof sqlite.batchSaveCostEntries>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).batchSaveCostEntries(...args)
    : sqlite.batchSaveCostEntries(...args);
}

export async function loadCostTotalAsync(...args: Parameters<typeof sqlite.loadCostTotal>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadCostTotal(...args)
    : sqlite.loadCostTotal(...args);
}

export async function loadCostEntriesAsync(...args: Parameters<typeof sqlite.loadCostEntries>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadCostEntries(...args)
    : sqlite.loadCostEntries(...args);
}

export async function loadCostEntriesInRangeAsync(
  ...args: Parameters<typeof sqlite.loadCostEntriesInRange>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadCostEntriesInRange(...args)
    : sqlite.loadCostEntriesInRange(...args);
}

export async function cleanOldCostEntriesAsync(
  ...args: Parameters<typeof sqlite.cleanOldCostEntries>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).cleanOldCostEntries(...args)
    : sqlite.cleanOldCostEntries(...args);
}

export async function deleteCostEntriesAsync(...args: Parameters<typeof sqlite.deleteCostEntries>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteCostEntries(...args)
    : sqlite.deleteCostEntries(...args);
}

export async function deleteAllCostDataAsync(...args: Parameters<typeof sqlite.deleteAllCostData>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteAllCostData(...args)
    : sqlite.deleteAllCostData(...args);
}

export async function saveLockoutStateAsync(...args: Parameters<typeof sqlite.saveLockoutState>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveLockoutState(...args)
    : sqlite.saveLockoutState(...args);
}

export async function loadLockoutStateAsync(...args: Parameters<typeof sqlite.loadLockoutState>) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadLockoutState(...args)
    : sqlite.loadLockoutState(...args);
}

export async function deleteLockoutStateAsync(
  ...args: Parameters<typeof sqlite.deleteLockoutState>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteLockoutState(...args)
    : sqlite.deleteLockoutState(...args);
}

export async function loadAllLockedIdentifiersAsync(
  ...args: Parameters<typeof sqlite.loadAllLockedIdentifiers>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadAllLockedIdentifiers(...args)
    : sqlite.loadAllLockedIdentifiers(...args);
}

export async function saveCircuitBreakerStateAsync(
  ...args: Parameters<typeof sqlite.saveCircuitBreakerState>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).saveCircuitBreakerState(...args)
    : sqlite.saveCircuitBreakerState(...args);
}

export async function loadCircuitBreakerStateAsync(
  ...args: Parameters<typeof sqlite.loadCircuitBreakerState>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadCircuitBreakerState(...args)
    : sqlite.loadCircuitBreakerState(...args);
}

export async function loadAllCircuitBreakerStatesAsync(
  ...args: Parameters<typeof sqlite.loadAllCircuitBreakerStates>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).loadAllCircuitBreakerStates(...args)
    : sqlite.loadAllCircuitBreakerStates(...args);
}

export async function deleteCircuitBreakerStateAsync(
  ...args: Parameters<typeof sqlite.deleteCircuitBreakerState>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteCircuitBreakerState(...args)
    : sqlite.deleteCircuitBreakerState(...args);
}

export async function deleteAllCircuitBreakerStatesAsync(
  ...args: Parameters<typeof sqlite.deleteAllCircuitBreakerStates>
) {
  const database = getSelectedPostgresDatabase();
  return database
    ? new PostgresDomainStateRepository(database).deleteAllCircuitBreakerStates(...args)
    : sqlite.deleteAllCircuitBreakerStates(...args);
}
