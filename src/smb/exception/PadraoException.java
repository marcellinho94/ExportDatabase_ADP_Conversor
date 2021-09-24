package smb.exception;

import smb.utils.Utilitario;

/**
 * Exceção específica da parte de negócio.
 * 
 * @author Wesley Camilo
 * 
 */
public class PadraoException extends Exception {

	private static final long serialVersionUID = 1L;
	private ListaMensagensRetorno listaMensagensRetorno;

	public PadraoException() {
		super();
	}

	public PadraoException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * NAO USAR ESTE CONSTRUTOR! USE UM DOS CONSTRUTORES ABAIXO:
	 * <ul>
	 * <li>throw new PadraoException(ListaMensagensRetorno)</li>
	 * <li>throw new PadraoException(MsgRetorno)</li>
	 * </ul>
	 * 
	 * @param message
	 */
	protected PadraoException(String message) {
		super(message);
	}

	public PadraoException(Throwable cause) {
		super(cause);
	}

	public PadraoException(ListaMensagensRetorno messages) {
		super();
		this.listaMensagensRetorno = messages;
	}

	public PadraoException(MsgRetorno message) {
		super();

		if (listaMensagensRetorno == null) {
			listaMensagensRetorno = new ListaMensagensRetorno();
		}
		this.listaMensagensRetorno.add(message);
	}

	public ListaMensagensRetorno getMensagensRetorno() {
		return listaMensagensRetorno;
	}

	@Override
	public String getMessage() {

		// Retornando a mensagem que existe na Super Classe
		if (!Utilitario.StringNullOuVaziaComTrim(super.getMessage()))
			return super.getMessage();

		// Retornando as mensagens que existem na lista
		if (!Utilitario.listaVazia(this.listaMensagensRetorno)) {
			String s = "";
			for (int i = 0; i < this.listaMensagensRetorno.size(); i++) {
				if (i > 0) {
					s += "\n";
				}
				s += this.listaMensagensRetorno.get(i).getDescricaoMensagem();
			}
			return s;
		}

		// Se não existem mensagens, retorna null
		return null;
	}
}
