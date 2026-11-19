const CLOUD_API_URL = "https://vlab-raft-cloud.onrender.com";

export async function fetchClusterState() {
  try {
    const response = await fetch(`${CLOUD_API_URL}/api/cluster`);
    return await response.json();
  } catch (error) {
    console.error("Failed to fetch cloud cluster state:", error);
    return null;
  }
}

export async function sendClientWrite(key: string, value: string) {
  try {
    const response = await fetch(`${CLOUD_API_URL}/api/client/set`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key, value }),
    });
    return await response.json();
  } catch (error) {
    console.error("Failed to send write to cloud:", error);
    return null;
  }
}

export async function sendClientRead(key: string) {
  try {
    const response = await fetch(`${CLOUD_API_URL}/api/client/get/${key}`);
    return await response.json();
  } catch (error) {
    console.error("Failed to read from cloud:", error);
    return null;
  }
}

export async function triggerChaosAction(action: string, nodeId?: string) {
  try {
    const response = await fetch(`${CLOUD_API_URL}/api/chaos/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nodeId }),
    });
    return await response.json();
  } catch (error) {
    console.error("Failed to execute chaos action:", error);
    return null;
  }
}
