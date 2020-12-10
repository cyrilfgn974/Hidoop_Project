package config;

/**
 * Java parameters for the project.
 */
public interface Configuration {
    //On teste en local
    public static int portWorker = 4000;

	public static int[] portsServeurs = {4500, 5000, 5500};
    public static String nomServeurNameNode = "localhost";
	public static String[] nomServeurDataNode = {"localhost", "localhost", "localhost"};
}