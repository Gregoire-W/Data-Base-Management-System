package premierPaquet;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.LocalTime;
import java.util.ArrayList;


public class BufferManager implements Serializable{
	
	private static final long serialVersionUID = -7328209705449282191L;
	private ArrayList<Frame> cases;	//liste de frame
	private static BufferManager gInstance = new BufferManager();	//crée une unique instance de BufferManager
	
	
	/*Constructeur qui initialise le BufferManager
	 */
	private BufferManager() {
		cases = new  ArrayList<>();
		for(int i=0;i<DBParams.frameCount;i++) {
			cases.add(new Frame());
		}
	}
	
	/*Getter gInstance
	 * @return gInstance un BufferManager
	 */
	public static BufferManager getBufferManager() {
		return(gInstance);
	}
	
	/*Méthode qui doit répondre a une demande de page venant des couches plus hautes et donc de retourner un des buffers associés à une frame
	 * @param page un PageId
	 * @return ByteBuffer
	 */
	public ByteBuffer GetPage(PageId page) {
		boolean pageSeTrouveEnMemoire = false;
		int l = 0;
		while(pageSeTrouveEnMemoire== false && l < cases.size()) {
			if(cases.get(l).getPage() != null && cases.get(l).getPage().getPageIdx() == page.getPageIdx() && cases.get(l).getPage().getFileIdx() == page.getFileIdx()) {
				pageSeTrouveEnMemoire = true;
				//La page se trouve en mémoire dans la case n° l
			}
			else {
				l++;
			}
		}
		if(pageSeTrouveEnMemoire == true) {
			//Nouvelle Utilisation de la PageID (fichier -->"+page.getFileIdx()+" page-->"+page.getPageIdx()+"), le nombre d'utilisateur augmentr
			cases.get(l).incrementerPinCount();
			//Nombre d'utilisateur actuelle de la page: cases.get(l).getPinCount()
			return(cases.get(l).getBuffer());
		}
		else {
			DiskManager DiskManag = DiskManager.getDiskManager();
			
			boolean CaseSansPage = false;
			int i =0;
			while(CaseSansPage == false && i<cases.size()) {
				if(cases.get(i).getPage() == null) {
					CaseSansPage = true;
				}
				else {
					i++;
				}
			}
			
			if(CaseSansPage == true) {
				cases.get(i).setPage(page);
				cases.get(i).incrementerPinCount();
				DiskManag.ReadPage(cases.get(i).getPage(),cases.get(i).getBuffer());
				return(cases.get(i).getBuffer());
				
			}
			
			else {
				
				int nombreCasePinCountVautZero=0;
				for(int j =0;j<cases.size();j++) {
					if(cases.get(j).getPinCount()==0) {
						nombreCasePinCountVautZero++;
					}
				}
				
				switch (nombreCasePinCountVautZero) {
				case 1:
					//Le BufferManager constate qu'il y actuellement une case qui contient une page non utilisé
					boolean trouverCasePinCountVautZero = false;
					int k=0;
					while(trouverCasePinCountVautZero == false) {
						if(cases.get(k).getPinCount()==0) {
							trouverCasePinCountVautZero = true;
						}
						else {
							k++;
						}
					}
					//Il s'agit de la PageId fichier-->cases.get(k).getPage().getFileIdx() page-->cases.get(k).getPage().getPageIdx(), elle est situé dans la case n° k
					cases.get(k).setPage(page);
					cases.get(k).incrementerPinCount();
					cases.get(k).clearBuffer();
					DiskManag.ReadPage(cases.get(k).getPage(),cases.get(k).getBuffer());
					return(cases.get(k).getBuffer());
			        
				case 0:
					//Toutes les pages sont actuellements en cours d'utilisation... Il n'y plus de place disponible pour votre opération
					ByteBuffer bufferError = ByteBuffer.allocate(1);
					return(bufferError);
					
				default:
					//Le BufferManager constate que plusieurs cases contiennent des pages non utilisés
					LocalTime unpinnedLeMoinsRecemment = LocalTime.MAX;
					
					for(int p =0;p<cases.size() ;p++) {
						if(cases.get(p).getPinCount() == 0){
						}
						if(cases.get(p).getPinCount() == 0 && unpinnedLeMoinsRecemment.isAfter(cases.get(p).getTime())) {
							unpinnedLeMoinsRecemment = cases.get(p).getTime();
						}
					}
					boolean caseAvecCeTemps = false;
					int indexCase = 0;
					while(caseAvecCeTemps == false) {
						if(cases.get(indexCase).getPinCount() == 0 && unpinnedLeMoinsRecemment.equals(cases.get(indexCase).getTime()) == true ) {
							caseAvecCeTemps = true;
						}
						else {
							indexCase ++;
						}
					}
					
					//Donc d'après l'algorithme de remplacement  LRU 'le plus anciennement unpinned' 
					//La case  indexCase contenant la PageId inutilisée (fichier--> cases.get(indexCase).getPage().getFileIdx() page-->cases.get(indexCase).getPage().getPageIdx()) sera remplacé;
					cases.get(indexCase).setPage(page);
					cases.get(indexCase).incrementerPinCount();
					cases.get(indexCase).clearBuffer();
					DiskManag.ReadPage(cases.get(indexCase).getPage(),cases.get(indexCase).getBuffer());
					return(cases.get(indexCase).getBuffer());
					
						
					
					
				}
				
				
					
				
				
				
			}
		}
		
	}

	/*Méthode qui permet de décrementer le pin_count et actualiser le flag dirty de la page
	 * @param page un PageId
	 * @param valdirty un boolean
	 * 
	 */
	public void FreePage(PageId page, boolean valdirty) {
		boolean pageSeTrouveDansCetteCase = false;
		DiskManager DiskMana = DiskManager.getDiskManager();

		int indexCase =0;
		while(pageSeTrouveDansCetteCase == false) {	//tant que la page ne se trouve pas dans cette case
			if(cases.get(indexCase).getPage().getFileIdx() == page.getFileIdx() && cases.get(indexCase).getPage().getPageIdx()== page.getPageIdx()) {
				pageSeTrouveDansCetteCase =true;
			}
			else {
				indexCase++;
			}
		}
		cases.get(indexCase).decrementerPinCount();	//on décremente le pin_count
		if(cases.get(indexCase).getPinCount() ==0) {
			LocalTime temps = LocalTime.now();
			cases.get(indexCase).setTime(temps);
			
		}
		cases.get(indexCase).setFlagDirty(valdirty);
		if(cases.get(indexCase).getFlagDirty() == true) {
			DiskMana.WritePage(cases.get(indexCase).getPage(), cases.get(indexCase).getBuffer());
		}
		
	}
	
	/*Méthode qui permet l’écriture de toutes les pages dont le flag dirty = 1 sur disque et
	 *  la remise à 0 de tous les flags/informations et contenus des buffers
	 * 
	 */
	public void FlushAll() {

		DiskManager DiskMana = DiskManager.getDiskManager();
		for(int indexCase =0;indexCase<cases.size();indexCase++) {
			if(cases.get(indexCase).getFlagDirty() == true) {
				DiskMana.WritePage(cases.get(indexCase).getPage(), cases.get(indexCase).getBuffer());
			}
			
			cases.get(indexCase).setPage(null);						//reinitialise la page
			cases.get(indexCase).reInitialiserFlagDirty();			//reinitialise le flag
			cases.get(indexCase).reInitialiserPinCount();			//reinitialise le pin_count
			cases.get(indexCase).clearBuffer();
			cases.get(indexCase).reInitialiserBuffer();
			cases.get(indexCase).setTime(null);
		}
	}
	
	
	/*
	 * Méthode qui permet d'initialiser le BufferManager
	 *
	 */
	public void Init() {
		String nomFichier = "BufferManager.sv";
		File fichierBufferManagersv = new File(DBParams.DBPath+"/"+nomFichier);
		if(fichierBufferManagersv.exists() == true) {
			FileInputStream fisBufferManagersv = null;
			ObjectInputStream oisBufferManagersv = null;
			try {
				fisBufferManagersv = new FileInputStream(fichierBufferManagersv);
				try {
					oisBufferManagersv = new ObjectInputStream(fisBufferManagersv);
				} catch (IOException e) {
					System.out.println("Erreur d'entrée/sortie initilialisation ObjectInputStream: "+e.getMessage());
					e.printStackTrace();
				}
				try {
					BufferManager.setGInstance((BufferManager) oisBufferManagersv.readObject());
				} catch (ClassNotFoundException | IOException e) {
					e.printStackTrace();
				}
			}catch(FileNotFoundException e) {
				System.out.println("Fichier non trouvé: "+e.getMessage());
				e.printStackTrace();
			}
			
			DiskManager DiskMana = DiskManager.getDiskManager();
			BufferManager bufferMana = BufferManager.gInstance;
			for(int i = 0; i<bufferMana.cases.size(); i++) {
				if((bufferMana.cases.get(i).getPage()) != null){
					bufferMana.cases.get(i).setBuffer(ByteBuffer.allocate(DBParams.PageSize));
					DiskMana.ReadPage(bufferMana.cases.get(i).getPage(), bufferMana.cases.get(i).getBuffer());
				}
				else {
					bufferMana.cases.get(i).setBuffer(ByteBuffer.allocate(DBParams.PageSize));
					//System.out.println("\nIl n'y avait pas de page sur la case"+(i+1));
				}
			}
			System.out.println("\nRécuperation du BufferManager à partir du fichier de sauvgarde \"BufferManager.sv\""); 
		}
		else {
			System.out.println("\nPhase d'initialisation du BufferManager\n");
			System.out.println("\nLe BufferManager ne possédait aucune sauvegarde\n");
			fichierBufferManagersv.delete();
		}
	}
	
	
	/*Méthode qui permet au contraire de terminer et sauvegarder le BufferManager
	 * 
	 */
	public void Finish() {
		String nomFichier = "BufferManager.sv";
		FileOutputStream fosBufferManagersv = null;
		ObjectOutputStream oosBufferManagersv = null;
		File fichierBufferManagersv = new File(DBParams.DBPath+"/"+nomFichier);
		try {
			
			fosBufferManagersv = new FileOutputStream(fichierBufferManagersv);

			try {
				oosBufferManagersv = new ObjectOutputStream(fosBufferManagersv);
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie initilialisation ObjectOutputStream: "+e.getMessage());
				e.printStackTrace();
			}
			
			try {
				oosBufferManagersv.writeObject(BufferManager.getBufferManager());
				
			} catch (IOException e) {
				System.out.println("Erreur d'entrée/sortie écriture ObjectOutputStream: ");
				e.printStackTrace();
			}
			//System.out.println("Enregistrement du BufferManager dans le fichier de sauvegarde \"BufferManager.sv\"");
			
		}catch(FileNotFoundException e ) {
			System.out.println("Fichier non trouvé ");
		}
		
		this.FlushAll();
        
	}
	
	/*Méthode qui réinitialise la liste de frames 
     * 
     */
	public void reset() {
		File fichierASupprimer;
		for(int indexCase =0;indexCase<cases.size();indexCase++) {
			cases.get(indexCase).setPage(null);
			cases.get(indexCase).reInitialiserFlagDirty();
			cases.get(indexCase).reInitialiserPinCount();
			cases.get(indexCase).clearBuffer();
			cases.get(indexCase).reInitialiserBuffer();
			cases.get(indexCase).setTime(null);
		}
		String cheminFichier = DBParams.DBPath+"/"+"BufferManager.sv";
        fichierASupprimer = new File(cheminFichier);
        if(fichierASupprimer.delete() == false) {
        	System.out.println("BufferManager.sv n'existant pas, il ne sera supprimé... -> méthode reset() du BufferManager");
        }
	}
	
	/*Setter gInstance
	 * @param newBufferManager un BufferManager
	 *
	 */
	private static void setGInstance(BufferManager newBufferManager) {
		gInstance = newBufferManager;
	}
	
	
}
