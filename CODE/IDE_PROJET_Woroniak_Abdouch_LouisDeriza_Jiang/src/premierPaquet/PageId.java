package premierPaquet;

import java.io.Serializable;

public class PageId implements Serializable{
	
	private static final long serialVersionUID = -189337442029316559L;
	private int FileIdx;	//identifiant du fichier
    private int PageIdx;	//indice de la page dans le fichier
    
    
    /*Constructeur qui prends en paramètre deux entiers qui initialise FileIdx et PageIdx
     * @param FileIdx un entier
     * @param PageIdx un entier
     * @return un phrase d'erreur si les paramètre ne respectent pas les conditions.
     */
    public PageId(int FileIdx, int PageIdx) {
        if(FileIdx >=0 && PageIdx < DBParams.maxPagesPerFile) {
            this.FileIdx = FileIdx;
            this.PageIdx =PageIdx;
        }
        else {
            System.out.println("Le page ne peut être intialisée\n");
        }

    }

    //Constructeur par défaut qui initialise les deux attributs à 0
    public PageId() {
        this.FileIdx = 0;
        this.PageIdx = 0;
    }

   /*Getter FileIdx
    * @return FileIdx un entier
    */
    public int getFileIdx() {
        return (FileIdx);
    }

    /*Getter PageIdx
     * @return PageIdx un entier
     */
    public int getPageIdx() {
        return (PageIdx);
    }

    /*Setter FileIdx
     * @param FileIdx un entier
     * @return void
     */
    public void setFileIdx(int FileIdx) {
        if(FileIdx >=0) {
            this.FileIdx = FileIdx;
        }
        else {
            System.out.println("Le paramètre FileIdx ne peut être modifiée\n");
        }
    }

    /*Setter PageIdx
     * @param PageIdx un entier
     * @return void
     */
    public void setPageIdx(int PageIdx) {
        if(PageIdx <=DBParams.maxPagesPerFile) {
            this.PageIdx = PageIdx;
        }
        else {
            System.out.println("Le paramètre PageIdx ne peut être modifiée\n");
        }
    }

    /*Setter PageIdx
     * @param FileIdx un entier
     * @param PageIdx un entier
     * @return void
     */
    public void setPageId(int FileIdx, int PageIdx) {
        if(FileIdx >=0 && PageIdx <= DBParams.maxPagesPerFile) {
            this.FileIdx = FileIdx;
            this.PageIdx =PageIdx;
        }
        else {
            System.out.println("Le page ne peut être modifée\n");
        }
    }
    
    /*Méthode qui retourne sous forme de String la valeur du FileIdx et du PageIdx
     * @return String
     */
    public String toString() {
    	StringBuilder strb = new StringBuilder("FileId: ");
    	strb.append(this.FileIdx+", ");
    	strb.append("PageId: "+this.PageIdx+"\n");
    	return(strb.toString());
    }

}
