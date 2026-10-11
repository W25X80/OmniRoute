export const servingFileIoFixture = {
  authError: null as Response | null,
  authCalls: 0,
};
export async function requireManagementAuth(_request: Request): Promise<Response | null> {
  servingFileIoFixture.authCalls++;
  return servingFileIoFixture.authError;
}
