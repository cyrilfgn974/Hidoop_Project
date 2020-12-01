package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import map.MapReduce;

public class Job implements JobInterface {
    //INT
    private int MapNb;
    private int ReduceNb;
    //String
	private String inputFName;
	private String resReduceFName;
    private String outputFName;
    private String interFName;

    //Format Type
	private Format.Type inputFormat;
    private Format.Type outputFormat;
    private Format.Type interFormat;
    private Format.Type resReduceFormat;


	private SortComparator sortComparator;
	private List<String> machines; //la liste des machines sur lesquelles tournent les démons
    
    public Job(Format.Type inputFormat, String inputFName) {
		this.inputFormat = inputFormat;
        this.inputFName = inputFName;
        
        this.outputFormat = Format.Type.KV;;
        this.outputFName = inputFName + "-map";

        this.interFormat = Format.Type.KV;;
        this.interFName = inputFName + "-inter";
        
        this.resReduceFormat = Format.Type.KV;
		this.resReduceFName = inputFName + "-resf";
		
    }
    
    public void startJob(MapReduce mr)
    {
        System.out.println("--------- HIDOOP : Lancement de la procédure startJob ---------\n");

        // Création des formats
        //on aura un format output (sortie du map) et un format (resReduce) qui contiendra les résultats
        //On récupère la liste des démons Workers
        System.out.println("------ HIDOOP : La liste des démons est en cours de récupération ------\n");
        List<Daemon> demons = new ArrayList<>();
        for(int i = 0; i < this.MapNb; i++) {
			try {
    		    
    			System.out.println("Connexion à : " + "//localhost:1999/" + machines.get(i));
				demons.add((Daemon) Naming.lookup("//localhost:1999/" + machines.get(i)));
				//demons.add((Daemon) Naming.lookup("//localhost/premierDaemon"));
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        System.out.println("------ HIDOOP : La liste des démons a bien été récupérée ------\n");


        //Initialisation CallBack
        System.out.println("------ HIDOOP : Initialisation du CallBack en cours ------\n");
        Callback CallBack = null;
		try {
			CallBack = new CallbackImpl(getNumberOfMaps());
		} catch (RemoteException e) {
			e.printStackTrace();
		}
        System.out.println("------ HIDOOP : Le CallBack est initialisé ------\n");




        //Lancement des map sur chaque démons Worker
        System.out.println("------ HIDOOP : Lancement des Map sur chaque Worker ------\n");
        for (int i = 0; i<=getNumberOfMaps(); i++)
        {   
            //TODO
        }
        System.out.println("------ HIDOOP : Les Map ont été effectués ------\n");

        // Attendre la fin du travail des démons
        System.out.println("------ HIDOOP : Attente du CallBack des Workers ------\n");
        try {
			CallBack.attendreMapOk();
		} catch (RemoteException e) {
            e.printStackTrace();
        }
		System.out.println("Callback reçu !!\n");
        System.out.println("------ HIDOOP : Le CallBack des Workers ont bien été réceptionnés ------\n");



        //HDFS : appel de la procédure read pour récupérer le résultat des map
        System.out.println("------ HDFS : Récupération de tous les résultats avec read ------\n");
        try {
			HdfsClient.HdfsRead(getInputFname() , getOutputFname());
		} catch (Exception e) {
			e.printStackTrace();
		} 
        System.out.println("------ HDFS :Les résultats ont bien été récupérés dans le fichier ------\n");



        //Lancement du réduce
        System.out.println("------ HIDOOP :Lancement de la procédure Réduce ------\n");
        System.out.println("--- Reduce : Ouverture du fichier contenant les résultats du map (concaténés) (readOnly) ---");
        output.open(Format.OpenMode.R);
        System.out.println("--- Reduce : Ouverture du fichier dans lequel les résultats du reduce vont être écrits (Write) ---");
        resReduce.open(Format.OpenMode.W);
        System.out.println("--- Reduce : LANCEMENT EN COURS---");
        mr.reduce(output, resReduce);
        System.out.println("--- Reduce : FIN ---");
        System.out.println("--- Reduce : Fermeture des fichiers précédemments ouverts ---");
        output.close();
		resReduce.close();
        System.out.println("------ HIDOOP : Fin de la procédure Réduce ------\n");
        System.out.println("--------- HIDOOP : Fin de la procédure startJob ---------\n");
  
    }

    public void setNumberOfMaps(int tasks)
    {
        this.MapNb=tasks;
    }
    public void setNumberOfReduces(int tasks)
    {
        this.ReduceNb=tasks;

    }
    //----
    public void setInputFormat(Format.Type ft)
    {
        this.inputFormat = ft;
    }
    public void setInputFname(String fname)
    {
        this.inputFName=fname;
    }
    //----
    public void setOutputFormat(Format.Type ft)
    {
        this.outputFormat = ft;
    }
    public void setOutputFname(String fname)
    {
        this.outputFName=fname;
    }
    //----
    public void setSortComparator(SortComparator sc)
    {
        this.sortComparator=sc;
    }

    public int getNumberOfMaps()
    {
        return this.MapNb;
    }
    public int getNumberOfReduces()
    {
        return this.ReduceNb;
    }
    //----
    public Format.Type getInputFormat()
    {
        return this.inputFormat;
    }
    public String getInputFname()
    {
        return this.inputFName;
    }
    //----
    public Format.Type getOutputFormat()
    {
        return this.outputFormat;
    }
    public String getOutputFname()
    {
        return this.outputFName;
    }
    public SortComparator getSortComparator()
    {
        return this.sortComparator;
    }
}