package premierPaquet;



public class SelectCommand {
	private RecordIterator recordIterator;
	
	/*Constructeur qui découpe la commande pour stocker les informations utiles.
	 */
	public SelectCommand(String commande) {
		SelectCondition selectCondition = new SelectCondition(commande);
		this.recordIterator  = new RecordIterator(selectCondition);
	}
	
	/*Méthode qui execute la commande select et affiche le résultat attendu
	 * @return void
	 */
	public void execute() {
		if(recordIterator.getCondition().getRelation() != null) {
			Record record;
			boolean finAffichage =false;
			int totalRecords = 0;
			while(finAffichage == false) {
				record = recordIterator.nextRecord();
				if(record == null) {
					finAffichage =true;
					System.out.println("Total records="+totalRecords);
				}
				else {
					System.out.println(record+".");
					totalRecords++;
					
				}
			}
			
		}
		
	}

}
