// Browser storage shims: OPFS access and exclusive ownership locks, written against the
// raw navigator APIs so they behave identically in Window and Dedicated Worker contexts.
export function storageRoot() {
  return navigator.storage.getDirectory();
}

export async function removeTree(dir, name) {
  await dir.removeEntry(name, { recursive: true });
}

export function deleteDatabase(name) {
  indexedDB.deleteDatabase(name);
}

export function acquireLock(name) {
  return new Promise((resolve, reject) => {
    navigator.locks
      .request(name, { mode: "exclusive", ifAvailable: true }, (lock) => {
        if (!lock) {
          reject(new Error("filesystem root is owned by another context"));
          return;
        }
        return new Promise((release) => {
          resolve(release);
        });
      })
      .catch(reject);
  });
}
