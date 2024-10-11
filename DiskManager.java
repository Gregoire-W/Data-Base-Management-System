package premierPaquet;
import java.util.ArrayList;
import java.lang.StringBuffer;
import java.nio.ByteBuffer;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class DiskManager implements Serializable{
	private static final long serialVersionUID = 3475015352476726117L;
	private HashMap<Integer, ArrayList<PageId>> listeFichier;	//liste de tous les fichiers
	private ArrayList <PageId> ListeDesFichiersDisponibles;		//liste des fichiers disponibles ( ceux avec une page pas encore pleine)
	private static DiskManager gInstance = new DiskManager();	//attribut static permettant de faire un appel unique au Constructeur pour avoir un seul DiskManager
	
	/*Constructeur qui initialise les attributs listeFichier et ListesDesFichiersDisponibles
	 */
	private DiskManager() {
		listeFichier = new HashMap<Integer, ArrayList<PageId>>();
		ListeDesFichiersDisponibles = new ArrayList<PageId>();
    }
	
	/*Méthode permettant de retourner le diskManager
	 * @return gInstance un DiskManager 
	 */
	public static DiskManager getDiskManager() {
		return(gInstance);
	}
	
	/*Setter DiskManager
	 * @param newDiskManager un DiskManager
	 * @return void
	 */
    private static void setGInstance(DiskManager newDiskmanager) {
    	gInstance = newDiskmanager;
    }
	
    /*Methode qui alloue une page, c'est a dire réserver une nouvelle page à la demande d'une des couches au-dessus.
	 * @return PageId  correspondant à la page nouvellement rajoutée.
	 */
	public PageId AllocPage() {
		if(ListeDesFichiersDisponibles.isEmpty()==true) {	//si la liste des fichiers disponibles est vide
			int i = 0;
			while(i+1 <=  listeFichier.size() && listeFichier.get(i).size() == DBParams.maxPagesPerFile) {
				i += 1;
			}
			
			if(i + 1 > listeFichier.size()) {	//si les fichiers ont atteint leurs tailles maximale
				StringBuffer nomFichierStringBuffer = new StringBuffer("F");
	        	nomFichierStringBuffer.append(Integer.toString(i));
	            nomFichierStringBuffer.append(".bdda");
	            String nomFichier = nomFichierStringBuffer.toString();
	            String cheminFichier = DBParams.DBPath+"/"+nomFichier;
	            File fichier = new File(cheminFichier);	//on crée un nouveau fichier
	            try {
					RandomAccessFile nouveauFile = new RandomAccessFile(fichier, "rw");
					try {
						nouveauFile.setLength( (long) (DBParams.PageSize));
					} catch (IOException e) {
						e.printStackTrace();
					}
	                
	                try {
	                	nouveauFile.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
	            listeFichier.put(i, new ArrayList<PageId>());	
	            listeFichier.get(i).add(new PageId(i,0));	//on rajoute la page dans le nouveau fichier
				return listeFichier.get(i).get(0);


			}
			
			else {	
				int position = 0;
				StringBuffer nomFichierStringBuffer = new StringBuffer("F");
	            nomFichierStringBuffer.append(Integer.toString(i));
	            nomFichierStringBuffer.append(".bdda");
	            String nomFichier = nomFichierStringBuffer.toString();
	            try {
	                RandomAccessFile accedeAuFichier = new RandomAccessFile(DBParams.DBPath+"/"+nomFichier,"rw");
	                long tailleFichier;
					try {
						tailleFichier = accedeAuFichier.length();
						long nouvelleTaille = tailleFichier + (long) DBParams.PageSize;
						accedeAuFichier.setLength(nouvelleTaille);
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					try {
						accedeAuFichier.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
	                
	               
	            } catch (FileNotFoundException e) {
	                e.printStackTrace();
	        
	            }
	            int indexNouvellePage = listeFichier.get(i).size();
				listeFichier.get(i).add(new PageId(i, indexNouvellePage));
				for(int j = 0; j < listeFichier.get(i).size(); j++) {
					position = (listeFichier.get(i).get(j).getPageIdx() != indexNouvellePage)? position:j;
				}
				return listeFichier.get(i).get(position);
			}
		}
	else {	// un fichier n'a pas atteint sa taille maximale
		int FichierMin =400000,PageMin = 5,i=0;
	    boolean aTrouvePosition = false;
		PageId pageDisponible;
		
		/*Trouver la page Disponible du fichier Fx.bdda avec x le plus petit possible*/
		
		for(int j =0;j<ListeDesFichiersDisponibles.size();j++) {
            if(ListeDesFichiersDisponibles.get(j).getFileIdx()<FichierMin) {
                FichierMin = ListeDesFichiersDisponibles.get(j).getFileIdx();
            }
		}
		
		/*Trouver la page disponible la plus 'petite' de Fx.bdda"*/
		for(int j =0;j<ListeDesFichiersDisponibles.size();j++) {
			if(ListeDesFichiersDisponibles.get(j).getFileIdx() == FichierMin) {
				PageMin = (ListeDesFichiersDisponibles.get(j).getPageIdx()<PageMin)?(ListeDesFichiersDisponibles.get(j).getPageIdx()):PageMin;
			}
		}
		
		/*Trouver sa position dans l'attribut 'ListeDesFichiersDisponibles'*/
		while (aTrouvePosition == false) {
			if(ListeDesFichiersDisponibles.get(i).getFileIdx() == FichierMin && ListeDesFichiersDisponibles.get(i).getPageIdx() == PageMin) {
				aTrouvePosition = true;
			}
			else {
				i++;
			}
		}
		
		pageDisponible = ListeDesFichiersDisponibles.get(i);
		ListeDesFichiersDisponibles.remove(i);
		listeFichier.get(pageDisponible.getFileIdx()).add(pageDisponible);
		
		return pageDisponible;
    }    
	}
	
	
	/*Méthode qui permet de remplir l'argument tampon avec le contenu disque de la page identifiée par l'argument pageALire.
	 * @param pageALire un pageId
	 * @param tampon un ByteBuffer
	 * @return void
	 */
	public void ReadPage(PageId pageALire, ByteBuffer tampon){
		int numeroDePage = pageALire.getPageIdx();
		int nombreOctetDeDepartDeLecture =numeroDePage*DBParams.PageSize;
		RandomAccessFile accedeAuFichier;
		
		StringBuffer nomFichierStringBuffer = new StringBuffer("F");
        nomFichierStringBuffer.append(Integer.toString((pageALire.getFileIdx())));
        nomFichierStringBuffer.append(".bdda");
        String nomFichier = nomFichierStringBuffer.toString();
        
	
		try {
			accedeAuFichier = new RandomAccessFile(DBParams.DBPath+"/"+nomFichier,"rw");
			try {
				accedeAuFichier.seek((long) nombreOctetDeDepartDeLecture);
				accedeAuFichier.read(tampon.array());	//remplit le contenu dans le tampon
				accedeAuFichier.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
			
    }
	
	
	/*Methode qui écrit le contenu de l'argument tampon dans le fichier et à la position indiqués par l'argument pageDEcriture.
	 * @param pageDEcriture un pageId
	 * @param tampon un ByteBuffer
	 * @return void
	 */
	public void WritePage(PageId pageDEcriture, ByteBuffer tampon){
		int numeroDePage = pageDEcriture.getPageIdx();
		int nombreOctetDeDepartEcriture =numeroDePage*DBParams.PageSize;
		
		StringBuffer nomFichierStringBuffer = new StringBuffer("F");
        nomFichierStringBuffer.append(Integer.toString((pageDEcriture.getFileIdx())));
        nomFichierStringBuffer.append(".bdda");
        String nomFichier = nomFichierStringBuffer.toString();
        RandomAccessFile accedeAuFichier;
	
		try {
			accedeAuFichier = new RandomAccessFile(DBParams.DBPath+"/"+nomFichier,"rws");
			try {

				accedeAuFichier.seek((long) nombreOctetDeDepartEcriture);
				accedeAuFichier.write(tampon.array());	//écrit le contenu du tampon dans le fichier
				accedeAuFichier.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
			
    }
    
	/*Méthode qui permet de désallouer une page en la remettant dans la liste des pages disponibles.
	 * @param page un PageId
	 * @return void
	 */
    public void DeallocPage(PageId page) {
    	boolean aTrouve =false;
    	int i= 0;
    	while(aTrouve == false) {
            if(listeFichier.get(page.getFileIdx()).get(i).getPageIdx()==page.getPageIdx()) {	//on cherche la page à désallouer
            	aTrouve = true;
            }
            else {
            	i++;
            }

    	}
    	ListeDesFichiersDisponibles.add(listeFichier.get(page.getFileIdx()).get(i));	//on la remet dans la liste des pages disponibles
    	listeFichier.get(page.getFileIdx()).remove(i);	//enlever la page de la liste des pages utilisées
    	
    	
    	
    }
    
    /*Méthode qui permet de retourner le nombre de pages allouées auprès du DiskManager
     * @return int
     */
    public int GetCurrentCountAllocPages() {
    	int nombrePagesAlloue =0;
    	for(int i =0;i<listeFichier.size();i++) {	//compte le nombre courant de pages alloués
    		nombrePagesAlloue += listeFichier.get(i).size();
    	}
    	return(nombrePagesAlloue) ;
    }
    
    /* Méthode qui initialise le DiskManager
     * @return void
     */
    public void Init() {
		String nomFichier = "DiskManager.sv";
		File fichierDiskManagersv = new File(DBParams.DBPath+"/"+nomFichier);
		if(fichierDiskManagersv.exists() == true) {	//si le chemin du fichier est le bon
			FileInputStream fisDiskManagersv = null;
			ObjectInputStream oisDiskManagersv = null;
			try {
				fisDiskManagersv = new FileInputStream(fichierDiskManagersv);
				try {
					oisDiskManagersv = new ObjectInputStream(fisDiskManagersv);
				} catch (IOException e) {
					System.out.println("Erreur d'entrée/sortie initilialisation ObjectInputStream: "+e.getMessage());
					e.printStackTrace();
				}
				try {
					DiskManager.setGInstance((DiskManager) oisDiskManagersv.readObject());
					System.out.println("\nRécuperation du DiskManager à partir du fichier de sauvgarde \"DiskManger.sv\"");
				} catch (ClassNotFoundException | IOException e) {
					
					e.printStackTrace();
				}
			}catch(FileNotFoundException e) {
				System.out.println("Fichier non trouvé: "+e.getMessage());
				e.printStackTrace();
			}
		}
		else {
			System.out.println("\nPhase d'initialisation du DiskManager\n");
			System.out.println("\nAucune page ou fichier n'est en mémoire disque\n");
			fichierDiskManagersv.delete();
		}
	}
    
    /*Methode qui enregistre le DiskManager
     * @return void
     */
    public void Finish() {
		String nomFichier = "DiskManager.sv";
		FileOutputStream fosDiskManagersv = null;
		ObjectOutputStream oosDiskManagersv = null;
		File fichierDiskManagersv = new File(DBParams.DBPath+"/"+nomFichier);
		try {
			fosDiskManagersv = new FileOutputStream(fichierDiskManagersv);

			try {
				oosDiskManagersv = new ObjectOutputStream(fosDiskManagersv);
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie initilialisation ObjectOutputStream: "+e.getMessage());
				e.printStackTrace();
			}
			
			try {
				oosDiskManagersv.writeObject(DiskManager.getDiskManager());
				
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie écriture ObjectOutputStream: "+e.getMessage());
				e.printStackTrace();
			}
			//System.out.println("Enregistrement du DiskManager dans le fichier de sauvegarde \"DiskManager.sv\"");
		}catch(FileNotFoundException e) {
			System.out.println("Fichier non trouvé: "+e.getMessage());
		}
		
        
	}
    
    /*Méthode qui réinitialise la liste des fichiers et la liste des fichiers disponibles 
     * @return void
     */
    public void reset() {
    	
    	File fichierASupprimer;
    	
    	for(int i =0; i<listeFichier.size(); i++) {
    		StringBuffer nomFichierStringBuffer = new StringBuffer("F");
        	nomFichierStringBuffer.append(Integer.toString(i));
            nomFichierStringBuffer.append(".bdda");
            String nomFichier = nomFichierStringBuffer.toString();
            String cheminFichier = DBParams.DBPath+"/"+nomFichier;
            fichierASupprimer = new File(cheminFichier);
            if(fichierASupprimer.delete() == false) {
            	System.out.println("Problème de suppression de fichier, méthode reset() du DiskManager...");
            }
    		
    	}
    	String cheminFichier = DBParams.DBPath+"/"+"DiskManager.sv";
        fichierASupprimer = new File(cheminFichier);
        if(fichierASupprimer.delete() == false) {
        	System.out.println("DiskManager.sv n'existant pas, il ne sera supprimé... -> méthode reset() du DiskManager");
        }
        
        listeFichier = new HashMap<Integer, ArrayList<PageId>>();	//les listes préalablement vidées sont réinitialisées
        ListeDesFichiersDisponibles = new ArrayList<PageId>();		//les listes préalablement vidées sont réinitialisées
    }
    
   
}
