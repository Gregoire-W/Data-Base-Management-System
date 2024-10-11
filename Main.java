package premierPaquet;


import java.util.Scanner;
public class Main {
	public static void main(String[] args) {
		
		// Initialisation des divers paramètres du programme
		DBParams.DBPath = args[0];
		DBParams.frameCount = 2;
		DBParams.PageSize = 4096;
		DBParams.maxPagesPerFile = 4;
		
		
		
		DBManager dbmana = DBManager.getDBManager();
		dbmana.Init();
		
		Scanner sc = new Scanner(System.in);
		String CommandeUtilisateur;
		sc.nextLine();
		boolean sortieDuProgramme = false;
		String AvantDerniereCommandeUtilisateur = null;
		
		do {
			System.out.println("------Veuillez entrez votre commande sql------");
			CommandeUtilisateur = sc.nextLine();
			
			if(CommandeUtilisateur.equals("EXIT")) {
				sortieDuProgramme = true;
				if(AvantDerniereCommandeUtilisateur!= null) {
					if((AvantDerniereCommandeUtilisateur.equals("DROPDB")) == false) {
						
						dbmana.Finish();
					}
				}
			}
			else {
				dbmana.processCommand(CommandeUtilisateur);
			}
			AvantDerniereCommandeUtilisateur = CommandeUtilisateur;
		}while(sortieDuProgramme == false);
		sc.close();

	
	}

}
