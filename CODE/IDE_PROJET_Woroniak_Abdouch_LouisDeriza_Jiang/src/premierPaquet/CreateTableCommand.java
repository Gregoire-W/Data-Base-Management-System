package premierPaquet;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class CreateTableCommand {
	private String nomRelation;
	private int nbColonnes;
	private ArrayList<String> nomColonnes;	//liste contenant le nom des colonnes
	private ArrayList<String> typesColonnes;	//liste contenant le type de chaque colonne

	/*Constructeur qui prends une commande en argument et qui la découpe en fonction des conditions présentes
	 * @param commande un String
	 */
	public CreateTableCommand(String commande) {

		nbColonnes = 0;
		StringTokenizer st1  = new StringTokenizer(commande,"() ,:");	//découpe la commande avec comme séparateur les deux parenthèses,l'espace,la virgule et les deux points
		int nbToken = st1.countTokens();
		String[] tableauStringCommande = new String[nbToken];
		for (int i =0; i<nbToken; i++) {
			tableauStringCommande[i] = st1.nextToken();
		}
		nomRelation = tableauStringCommande[2];	//stocke le nom de la relation 
		
		for (int i =3; i<nbToken; i++) {
			String morceauDeCommande = tableauStringCommande[i];
			if(morceauDeCommande.equals("INTEGER") || morceauDeCommande.equals("REAL") || morceauDeCommande.equals("VARCHAR")) {
				nbColonnes++;
			}
		}

	    nomColonnes = new ArrayList<>();
		typesColonnes = new ArrayList<>();
		
		int iteration = 3;
		for(int i = 0; i<nbColonnes; i++) {
			
			nomColonnes.add(tableauStringCommande[iteration]);
			
			String type = tableauStringCommande[iteration+1];
			if(type.equals("VARCHAR")){
				StringBuilder sb = new StringBuilder();
			    sb.append(type);
			    sb.append("(");
			    sb.append(tableauStringCommande[iteration+2]);
			    sb.append(")");
			    typesColonnes.add(sb.toString());
			    iteration+=3;
			    
			}
			else {
				typesColonnes.add(type);
				iteration+=2;
			}
			
			

		}
	}
	
	/*Méthode qui permet d'executer la commande du constructeur
	 * @return void
	 */
	public void execute() {
		ArrayList<ColInfo> colInfo= new ArrayList<ColInfo>();
		FileManager FileMana = FileManager.getFileManager();
		PageId headerpage = FileMana.createNewHeaderPage();
		for(int i=0; i<nbColonnes;i++) {
			colInfo.add(new ColInfo(nomColonnes.get(i),typesColonnes.get(i)));
		}
		
		for(ColInfo col : colInfo) {
			System.out.println(col);
		}
		RelationInfo relInfo = new RelationInfo(nomRelation,colInfo,headerpage);	//crée une relationInfo 
		Catalog catalog = Catalog.getCatalog();
		catalog.AddRelationInfo(relInfo);	//ajoute la relationInfo au catalog
		catalog.Finish();
		DiskManager diskMana  = DiskManager.getDiskManager();
		diskMana.Finish();
		BufferManager bufferMana = BufferManager.getBufferManager();
		bufferMana.Finish();

	}
}

