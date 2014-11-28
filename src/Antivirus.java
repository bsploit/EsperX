
public class Antivirus {
	private String compname;
	private String file;
	private String virusname;
	public Antivirus(String compname,String file,String virusname){
		this.compname=compname;
		this.file=file;
		this.virusname=virusname;
	}
	public String getCompname() {return compname;}	
	public String getFile() {return file;}
	public String getVirusname() {return virusname;}			
}
