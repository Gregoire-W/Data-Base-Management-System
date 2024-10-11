package premierPaquet;
import java.nio.ByteBuffer;
import java.util.ArrayList;


/*Cette classe va gérer tout ce qui touche aux fichiers à savoir la création de nouvelle page de donnée
 *ainsi que la lecture et l'écriture de Records d'une relation
 */
public class FileManager {
	
	private static FileManager gInstance = new FileManager();
	private FileManager() {
	}
	
	
    /*Cette méthode retourne la seule instance de FileManager ça permet de ne pas la dupliquer
     * @return FileManager le singleton FileManager
     */
	public static FileManager getFileManager() {
		return gInstance;
	}
	
	/*Méthode qui permet de créer une header page et de l'initialiser (mettre le nombre de page de la relation à 0)
	 * @return PageId de la headerPage créée
	 */
	public PageId createNewHeaderPage() {
		DiskManager diskMana = DiskManager.getDiskManager();
		PageId headerPageId= diskMana.AllocPage(); //Création de la page
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(headerPageId);
		byteBuffer.putInt(0); //On met le nombre de page de la relation à 0 
		bufferMana.FreePage(headerPageId, true);
		return headerPageId;
	}
	
	
	/*Méthode qui ajoute ne page vide de donnée à une relation donnée
	 * @param relInfo une RelationInfo 
	 * @return PageId de la page créée 
	 */
	public PageId addDataPage(RelationInfo relInfo ) {
		DiskManager diskMana = DiskManager.getDiskManager();
		PageId dataPageId= diskMana.AllocPage(); //Création de la page
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(dataPageId);
		byteBuffer.putInt(DBParams.PageSize-4, 0); //Initialisation de la position du début d'écriture à 0
		byteBuffer.putInt(DBParams.PageSize-8, 0); //Initialisation du nombre de record de la page à 0
		bufferMana.FreePage(dataPageId, true);
		ByteBuffer byteBuffer2 = bufferMana.GetPage(relInfo.getHeaderPageId());
		int nbrPage = byteBuffer2.getInt(0);
		byteBuffer2.putInt(0, nbrPage+1); //nombre de page + 1 pour la header page
		
		//On rentre les info de la page créée dans la header page (PageId et place dispo)
		byteBuffer2.putInt(4+nbrPage*12,dataPageId.getFileIdx());
		byteBuffer2.putInt(8+nbrPage*12,dataPageId.getPageIdx());
		byteBuffer2.putInt(12+nbrPage*12,DBParams.PageSize-8);
		bufferMana.FreePage(relInfo.getHeaderPageId(), true);
		return dataPageId;
	}
	
	/*Méthode qui renvoie la première page d'une relation donnée ayant la place nécessaire pour accueillir un record d'une taille donnée
	 * @param relInfo une RealtionInfo
	 * @param sizeRecord un entier (la taille du record à écrire)
	 * @return PageId de la page avec la place nécessaire ou null si une telle page n'existe pas
	 */
	public PageId getFreeDataPageId(RelationInfo relInfo, int sizeRecord) {
		int i=0;
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(relInfo.getHeaderPageId());
		int j=12;
		while(i<byteBuffer.getInt(0)) { //Pour chaque page de la relation 
			if((sizeRecord + 8)<byteBuffer.getInt(j)){ //Si la taille du record + 8 (pour le slot directory) est plus prtit que l'espace dispo dans la page
				//On prend le PageId de la page dans la headerPage
				int fileIdx= byteBuffer.getInt(j-8);
				int pageIdx= byteBuffer.getInt(j-4);
				bufferMana.FreePage(relInfo.getHeaderPageId(), true); 
				return new PageId(fileIdx,pageIdx); //Puis on retourne ce PageId
			}
			j+=12; //Pour passer à la page suivante dans la header page on avance de 12 octets
			i++;
		}
		bufferMana.FreePage(relInfo.getHeaderPageId(), false);
		return null; //Si aucune page n'a de place on retourne null
	}
	
	/*Méthode qui écrit une record donnée dans une page donnée
	 * @param record le Record à écrire
	 * @param pageId le PageId de la page d'écriture
	 * @return le RecordId du record écrit 
	 */
	public RecordId writeRecordToDataPage(Record record, PageId pageId,boolean reecriture) {
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(pageId);
		int pos = byteBuffer.getInt(DBParams.PageSize - 4); //On récupère la position d'écriture de la page
		record.writeToBuffer(byteBuffer,pos); //écriture du record
		byteBuffer.putInt(DBParams.PageSize-4,pos+record.getWrittenSize()); //actualistion de la position d'écriture de la page
		int nbrSlots=byteBuffer.getInt(DBParams.PageSize-8);
		if(!reecriture) {
			byteBuffer.putInt(DBParams.PageSize-8,nbrSlots+1); //+1 pour le nombre de record de la page
			byteBuffer.putInt((DBParams.PageSize - 16)- nbrSlots * 8,pos); //écriture position du record dans le slotDirectory
			byteBuffer.putInt((DBParams.PageSize - 12) - nbrSlots * 8,record.getWrittenSize()); //écriture taille du record dans le slotDirectory
		}
		bufferMana.FreePage(pageId, true);
		
		//Actualisation de la header page de la relation
		if(!reecriture) {
			ByteBuffer byteBuffer2 = bufferMana.GetPage(record.getRelInfo().getHeaderPageId());
			boolean trouve = false;
			int i = 4;
			while(trouve == false) { //On cherche la page sur laquelle on à écrit dans la header page
				if(byteBuffer2.getInt(i) == pageId.getFileIdx() && byteBuffer2.getInt(i+4) == pageId.getPageIdx()) { //Quand on trouve cette page
					int nbOctesLibresDeLaPage = byteBuffer2.getInt(i+8);
					byteBuffer2.putInt(i+8, nbOctesLibresDeLaPage - (record.getWrittenSize() + 8)); //On incrémente la lace disponible de la page par la taille du record + 8(pour le slot directory)
					trouve = true;
				}
				else {
					i+=12;
				}
			}
			bufferMana.FreePage(record.getRelInfo().getHeaderPageId(), true);
		}
		return new RecordId(pageId,nbrSlots);
	}
	
	
	/*Méthode qui renvoie tous les records d'une page donnée
	 * @param pageId le PageId de la page contenant les records à renvoyer
	 * @param relInfo l RelationInfo à laquelle appartient la page
	 * @return la liste de tous les records contenu dans pageId
	 */
	public ArrayList<Record> getRecordsInDataPage(RelationInfo relInfo,PageId pageId) {
		ArrayList<Record> listeDeRecords=new ArrayList<Record>();
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(pageId);
		int posRec;
		for(int i = 0; i < byteBuffer.getInt(DBParams.PageSize - 8);i++) { //Pour i allant de 0 au nombre de record - 1 de la page
			posRec = byteBuffer.getInt(DBParams.PageSize - 16 - i*8); //position du i-ème record à lire
			//On lit le record à la position posRec puis on l'ajoute à la liste
			listeDeRecords.add(new Record(relInfo,Record.readFromBufferOffset(byteBuffer, posRec),Record.readFromBufferValues(relInfo, byteBuffer, posRec)));
			
		}
		bufferMana.FreePage(pageId, false);
		return listeDeRecords;
	}
	
	/*Méthode qui renvoie toutes les page contenus dans une relation
	 * @param relInfo la RelationInfo dont on veut les pages
	 * @return une liste de PageId de toutes les pages de la relation
	 */
	public ArrayList<PageId> getAllDataPages(RelationInfo relInfo) {
		ArrayList<PageId> listeDePageIds = new ArrayList<PageId>();
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(relInfo.getHeaderPageId());
		for(int i=0;i<byteBuffer.getInt(0);i++) { //Pour i allant de 0 au nombre de page de la relation - 1
			//On ajoute à la liste la i-ème pageId dans la header page
			listeDePageIds.add(new PageId(byteBuffer.getInt(i*12+4),byteBuffer.getInt(i*12+8))); 
		}
		bufferMana.FreePage(relInfo.getHeaderPageId(),false);
		return listeDePageIds;
	}
	
	/*Méthode qui écrit un record dans une page consacré à sa relation
	 * @param record le Record à écrire
	 * @return RecordId du record qui a été écrit 
	 */
	public RecordId InsertRecordIntoRelation(Record record) {
		PageId pageIdLibre = this.getFreeDataPageId(record.getRelInfo(),record.getWrittenSize()); //On prend une page ayant la place pour le record
		if(pageIdLibre == null) { //Si il n'y en a pas on la créer
			pageIdLibre = this.addDataPage(record.getRelInfo());	
		}
		return this.writeRecordToDataPage(record,pageIdLibre,false); //On écrit le record dans cette page
	}
	
	/*Méthode qui renvoie tous les records d'une relation donnée 
	 * @param relInfo la RelationInfo dont on veut les records 
	 * @return une liste de Record de tous les records de la relation 
	 */
	public ArrayList<Record> GetAllRecords(RelationInfo relInfo){
		ArrayList<Record> listeDeRecords = new ArrayList<Record>();
		BufferManager bufferMana = BufferManager.getBufferManager();
		ByteBuffer byteBuffer = bufferMana.GetPage(relInfo.getHeaderPageId());
		for(int i=0;i<byteBuffer.getInt(0);i++) { //Pour i allant de 0 au nombre de page de la relation - 1
			//On récupère les records de la i-ème page
			ArrayList<Record> petiteListe= this.getRecordsInDataPage(relInfo,new PageId(byteBuffer.getInt(i*12+4),byteBuffer.getInt(i*12+8)));
			//On les ajoute tous à la liste de tous les records de la relation 
			for(int j=0;j<petiteListe.size();j++) {
				listeDeRecords.add(petiteListe.get(j));
			}
		}
		bufferMana.FreePage(relInfo.getHeaderPageId(), false);
		return listeDeRecords;
	}

}

