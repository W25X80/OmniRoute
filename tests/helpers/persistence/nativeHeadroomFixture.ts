export const headroomFixture = { url: "http://localhost:8787" };
export async function getCachedSettings() {
  return { headroomUrl: headroomFixture.url };
}
