package premierPaquet;
/**
 * Classe qui gere la commande DROPDB, permet de réinitialiser toutes les informations contenues dans notre SGBD
 * 
 *
 */
public class DropDBCommand {
	/**
	 * Permet de reinialiser le DiskManager, BufferManager et le Catalog
	 * 
	 * @author Mounir Abdouch, Grégoire woroniak, Socrate Louis Deriza, Olivier Jiang
	 * 
	 */
	public void execute() {
		DiskManager diskmana = DiskManager.getDiskManager();
		BufferManager buffermana = BufferManager.getBufferManager();
		Catalog catalog = Catalog.getCatalog();
		
		catalog.reset();
		buffermana.reset();
		diskmana.reset();
		System.out.println("Fin de la commande DROPDB");
		
	}

}