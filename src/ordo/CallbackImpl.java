package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallbackImpl extends UnicastRemoteObject implements Callback {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private Semaphore nbMapsTermines;

    public CallbackImpl() throws RemoteException {
        nbMapsTermines = new Semaphore(0);
    }

    @Override
    // Permet à un démons de confier qu'il a bien terminé son traitement de map
    public void confirmerFinMap() throws InterruptedException, RemoteException {
        nbMapsTermines.release();
    }

    @Override
    public void attendreFinMap(int nb) throws InterruptedException, RemoteException {
        for(int i = 0; i < nb; i++) {
			try {
				nbMapsTermines.acquire();
		    	System.out.println((i+1) + " maps se sont finis.");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
    }
}