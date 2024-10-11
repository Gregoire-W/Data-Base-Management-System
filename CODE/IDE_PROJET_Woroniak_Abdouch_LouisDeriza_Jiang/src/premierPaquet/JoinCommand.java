package premierPaquet;

/**
 * Represente la commande sql de jointure sur une base de données 
 * 
 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
 *
 */
public class JoinCommand {
	
	/**
	 * le generateur des records respectant conditions de la jointure
	 */
	private RecordIteratorJoin recordIteratorJoin;
	
	/**
	 * 
	 * Construit une commande de jointure à partir d'une chaine de caracetère
	 * 
	 * @param commande représentant la chaîne de caractère écrit par l'utilisateur
	 */
	public JoinCommand(String commande) {
		SelectConditionJoin selectConditionJoin = new SelectConditionJoin(commande);
		this.recordIteratorJoin = new RecordIteratorJoin(selectConditionJoin);
	}
	
	/**
	 * 
	 * execute la commande de jointure et le compte le nombre de record affiché
	 * 
	 */
	public void execute() {
		if(recordIteratorJoin.getCondition().getRelation() != null) {
			Record records[];
			boolean finAffichage =false;
			int totalRecords = 0;
			while(finAffichage == false) {
				records = recordIteratorJoin.pageOrientedNestedLoopsJoin();
				if(records == null) {
					finAffichage =true;
					System.out.println("Total records="+totalRecords);
				}
				else {
					System.out.println(records[0]+" ; "+records[1]+".");
					totalRecords++;
					
				}
			}
		}
	}

}
