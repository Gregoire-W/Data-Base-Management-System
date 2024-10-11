package premierPaquet;
/*Cette classe stock la page et la poition dans cette page d'un record écrit
*/

public class RecordId {
	private PageId pageId;
	private int slotIdx;
	public RecordId(PageId pageId, int slotIdx) {
		this.pageId = pageId;
		this.slotIdx = slotIdx;
	}
	
	/*Cette méthode renvoie le pageId de ce RecordId
	 * @return pageId le PageId du RecordId
	 */
	public PageId getPageId() {
		return pageId;
	}
	
	/*Cette méthode change le pageId de ce RecordId par la valeur donnée
	 * @param pageId le nouveau PageId
	 */
	public void setPageId(PageId pageId) {
		this.pageId = pageId;
	}
	
	/*Cette méthode renvoie le slotIdx de ce RecordId
	 * @return int la position du Record dans le SlotDirectory
	 */
	public int getSlotIdx() {
		return slotIdx;
	}
	
	/*Cette méthode change le slotIdx de ce RecordId par la valeur donnée
	 * @param int le nouveau slotIdx
	 */
	public void setSlotIdx(int slotIdx) {
		this.slotIdx = slotIdx;
	}
	

}
