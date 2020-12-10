  
package ordo;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Callback extends Remote {
    // Permet à un démons de confier qu'il a bien terminé son traitement de map
	public void confirmerFinMap() throws InterruptedException, RemoteException;

	// Permet de savoir si nb maps sont terminés
    public void attendreFinMap(int nb) throws InterruptedException, RemoteException;
}