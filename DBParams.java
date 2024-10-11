package premierPaquet;

/**
 * Classe qui contient les parametres de notre SGBD
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 * 
 */

public class DBParams {
	/**
	 * Le chemin qui mene a notre dossier DB
	 * 
	 */
	public static String DBPath;
	
	/**
	 * La taille d'une page
	 * 
	 */
    public static int PageSize;
    
    /**
     * Le nombre maximum de pages par fichier
     * 
     */
    public static int maxPagesPerFile;
    
    /**
     * Le nombre de frames de notre BufferManager
     * 
     */
    public static int frameCount;

}