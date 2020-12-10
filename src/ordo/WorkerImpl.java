package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject ;

import config.Configuration;
import formats.Format;
import map.Mapper;

public class WorkerImpl extends UnicastRemoteObject implements Worker
{
	
	private static final long serialVersionUID = 1L;
	private String adresseServeur;

	public WorkerImpl(String adresseServeur) throws RemoteException {
		this.adresseServeur = adresseServeur;
	}


    @Override
    public void runMap(Mapper m, Format reader, Format writer, Callback cb) throws RemoteException {
        LanceurMap lanceur = new LanceurMap(m, reader, writer, cb);
		lanceur.start();     

    }
    //procédures utiles au lancement des workers sur les machines

	public String getAdresseServeur() 
	{
		return this.adresseServeur;
	}
    
	//Lancement des Workers sur tous les serveurs
	public static void main(String[] args)
	{
		InetAddress adresse;
		if (args.length>0)
		{
			try {
				//Création du registre RMI
				LocateRegistry.createRegistry(Configuration.portWorker);
				System.out.println("Le registre RMI a bien été créé");
			} catch (Exception e) {
				System.out.println("Le registre RMI a déjà été créé");
			}

			try 
			{
				//Lier le Worker au registre du RMI
				adresse = InetAddress.getLocalHost();
				WorkerImpl worker = new WorkerImpl(args[0]);
				Naming.rebind("//"+worker.getAdresseServeur()+":"+Configuration.portWorker+"/DaemonImpl",worker);
				System.out.println("le Worker a bien été lié au registre du RMI");
			} catch (Exception e) 
			{
				e.printStackTrace();
			}
		} else
		{
			System.out.println("Revoir les arguments");
		}
	}
}	