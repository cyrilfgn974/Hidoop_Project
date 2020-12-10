package ordo;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;

import java.util.List;

import config.Configuration;
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
    private String outputFName;
	private String resReduceFName;
    private String interFName;
    //Format Type
	private Format.Type inputFormat;
    private Format.Type outputFormat;
    private Format.Type resReduceFormat;
    private Format.Type interFormat;



	private SortComparator sortComparator;
	private List<String> machines; //la liste des machines sur lesquelles tournent les démons
    public Job() {
		//Initialisation des machines
		this.MapNb = 0;
		this.ReduceNb = 1; 
	}
    public Job(Format.Type inputFormat, String inputFName) {
		this.inputFormat = inputFormat;
        this.inputFName = inputFName;
        
        this.outputFormat = Format.Type.KV;;
        this.outputFName = inputFName + "-map";
        
        this.resReduceFormat = Format.Type.KV;
        this.resReduceFName = inputFName + "-resf";
        
        this.interFormat = Format.Type.KV;
        this.interFName = inputFName + "-inter";
		
    }
    
    public void startJob(MapReduce mr)
    {
        System.out.println("--------- HIDOOP : Lancement de la procédure startJob ---------\n");



        /*...........................................................................................................*/
        // Création des formats   OK
        Format input, inter, resReduce, output;
        if(inputFormat == Format.Type.LINE) { // LINE
			input = new LineFormat(inputFName);
		} else { // KV
			input = new KVFormat(inputFName);
		}
		inter = new KVFormat(interFName);
		resReduce = new KVFormat(resReduceFName);
		output = new KVFormat(outputFName);


        /*...........................................................................................................*/



        /*...........................................................................................................*/

        //On récupère la liste des démons Workers   A REVOIR
        System.out.println("------ HIDOOP : La liste des Workers est en cours de récupération ------\n");
        List<Worker> workerList = new ArrayList<>();
        for(int i = 0; i < this.MapNb; i++) {
			try {
    			System.out.println("Connexion à : //"+Configuration.nomServeurDataNode[i]+":4500/DaemonImpl"+ (i+1) );
				workerList.add((Worker) Naming.lookup("//"+Configuration.nomServeurDataNode[i]+":4500/DaemonImpl"+ (i+1) ));
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        System.out.println("------ HIDOOP : La liste des Workers a bien été récupérée ------\n");
        /*...........................................................................................................*/



        /*...........................................................................................................*/
        //Initialisation CallBack   OK
        System.out.println("------ HIDOOP : Initialisation du CallBack en cours ------\n");
        Callback cb = null;
		try {
			cb = new CallbackImpl();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
        System.out.println("------ HIDOOP : Le CallBack est initialisé ------\n");
        /*...........................................................................................................*/


        /*...........................................................................................................*/
        //Lancement des map sur chaque démons Worker  TO DO
        System.out.println("------ HIDOOP : Lancement des Map sur chaque Worker ------\n");
        for (int i = 0; i<=getNumberOfMaps(); i++)
        {   
            Worker d = workerList.get(i);
			// On change le nom des Formats en rajoutant un numéro pour que les fragments aient des noms différents pour chaque Daemon
			Format inputTmp;
	        if(inputFormat == Format.Type.LINE) { // LINE
	        	inputTmp = new LineFormat(getInputFname() + "" + i);
			} else { // KV
	        	inputTmp = new KVFormat(getInputFname() + "" + i);
			}
	        Format interTmp = new KVFormat(getInterFname() + "" + i);
	        
			try {

                d.runMap(mr, inputTmp, interTmp, cb);			
            } catch (RemoteException e) {
				e.printStackTrace();
			}
        }
        System.out.println("------ HIDOOP : Les Map ont été effectués ------\n");
        /*...........................................................................................................*/



        /*...........................................................................................................*/
        // Attendre la fin du travail des démons
        System.out.println("------ HIDOOP : Attente du CallBack des Workers ------\n");
        try {
			cb.attendreFinMap(MapNb);
		} catch (RemoteException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            
            e.printStackTrace();
        }
		System.out.println("Callback reçu !!\n");
        System.out.println("------ HIDOOP : Le CallBack des Workers ont bien été réceptionnés ------\n");
        /*...........................................................................................................*/




        /*...........................................................................................................*/
        //HDFS : appel de la procédure read pour récupérer le résultat des map
        System.out.println("------ HDFS : Récupération de tous les résultats avec read ------\n");
        try {
			HdfsClient.HdfsRead(getInterFname() , getOutputFname());
		} catch (Exception e) {
			e.printStackTrace();
		} 
        System.out.println("------ HDFS :Les résultats ont bien été récupérés dans le fichier ------\n");
        /*...........................................................................................................*/





        /*...........................................................................................................*/
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
        /*...........................................................................................................*/

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
    public void setResReduceFname(String fname){
		this.resReduceFName = fname;
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

	public String getResReduceFname() {
		return this.resReduceFName;
    }
    public String getInterFname()
    {
        return this.interFName;
    }
    public void setInterFName(String fname)
    {
        this.interFName=fname;
    }
}   