package premierPaquet;


/*
 * Cette classe va gérer l'initialisation et la fermeture de la base de donnée ainsi que l'arrivée des commandes
*/
public class DBManager {
	
    private static DBManager DBManagerInstance = new DBManager();
	
	/*Cette méthode retourne la seule instance de DBManager ça permet de ne pas la dupliquer
     * @return DBManager le singleton DBManager
     */
	public static DBManager getDBManager() {
		return(DBManagerInstance);
	}
	
	/*Cette méthode va initialiser la base de données
	*/
	public void Init() {
		
		//Initialistion de toutes les couches de la base de données
		DiskManager dm = DiskManager.getDiskManager();
		dm.Init();
		dm = DiskManager.getDiskManager();
		
		BufferManager bm =BufferManager.getBufferManager();
		bm.Init();
		bm = BufferManager.getBufferManager();
		
		Catalog c =Catalog.getCatalog();
		c.Init();
		c = Catalog.getCatalog();	
	}
	
	/*Cette méthode va fermer la base de données
	*/
	public void Finish() {
		
		//Fermeture des couches de la base de données
		BufferManager bm =BufferManager.getBufferManager();
		bm.Finish();
		
		Catalog c =Catalog.getCatalog();
		c.Finish();
		
		DiskManager dm = DiskManager.getDiskManager();
		dm.Finish();
		
		
		
		
		
		
		
	}
	
	/*Cette méthode va traiter le début d'une commande pour qu'elle soit ensuite gérer pr se classe respective
	 * @param commande un String d'une commande tapée par l'utilisateur 
	 */
	public void processCommand(String commande) {
		String[] chaine = commande.split(" ");
		if((chaine[0].equals("CREATE") && chaine[1].equals("TABLE") )) {
			CreateTableCommand createTable = new CreateTableCommand(commande);
			createTable.execute();
			
		}
		else if(chaine[0].equals("DROPDB")) {
			DropDBCommand drop = new DropDBCommand();
			drop.execute();
		}
		else if(chaine[0].equals("INSERT") && chaine[1].equals("INTO")) {
		    InsertCommand insertInto = new InsertCommand(commande);
			insertInto.execute();
		}
        else if(chaine[0].equals("SELECT") && chaine[3].contains(",")) {
        	JoinCommand join = new JoinCommand(commande);
        	join.execute();
		}
		else if(chaine[0].equals("SELECT")) {
			SelectCommand select = new SelectCommand(commande);
			select.execute();
			
		}
		else if(chaine[0].equals("DELETE")) {
			DeleteCommand delete = new DeleteCommand(commande);
			delete.deleteAllPages();
			
		}
		
		
	}

}
