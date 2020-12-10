package ordo;

import java.rmi.RemoteException;
import formats.Format;
import map.Mapper;


public class LanceurMap extends Thread 
{
    Mapper m; //map à lancer
	Format lecture, ecriture; //les formats de lecture et d'écriture
	Callback cb;

	public LanceurMap(Mapper m, Format r, Format w, Callback cb){
		this.m = m;
		this.lecture = r;
		this.ecriture = w;
		this.cb = cb;
	}

	public void run() {
		System.out.println("LanceurMap : Lancement du map");
		System.out.println("Ouverture du fichier contenant le fragment sur lequel exécuter le map");
		lecture.open(Format.OpenMode.R);
		System.out.println("Ouverture du fichier dans lequel les résultats du map vont être déposés ");
		ecriture.open(Format.OpenMode.W);
		System.out.println("Lancement du map sur le premier fichier");
		m.map(lecture, ecriture);
		System.out.println("Le map a été réalisé");
		System.out.println("Fermeture des fichiers en lecture et écriture ");
		lecture.close();
		ecriture.close();

		System.out.println("Envoie du callback ...");
		try {
			cb.confirmerFinMap();   
		} catch (RemoteException e) {  
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("LanceurMap a bien fonctionné."); 
	}
}
