package smb.utils;

import java.io.*;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

/**
 * Classe utilitaria.
 *
 * @version 1.0
 */
public class Utilitario {

  public static void main(String[] args) throws IOException {

    String folder =
        Utilitario.diretorioLocal()
            + File.separator
            + "BASE_AGRUPADA"
            + File.separator
            + "funcao.d";
    File file = new File(folder);
    byte[] readAllBytes = Files.readAllBytes(file.toPath());
    String fullFile = new String(readAllBytes);

    String[] textToLines = textToLines(fullFile);

    for (int i = 0; i < textToLines.length; i++) {
      System.out.println(textToLines[i]);
    }
  }

  public static boolean listaVazia(List<?> lista) {

    if (lista == null) {
      return true;
    }

    return lista.isEmpty();
  }

  /**
   * Verifica se um array unidimensional contem elementos e se esta instanciado.
   *
   * @param array Array unidimensional a ser verificado.
   * @return Retorna true se nao estiver instanciado ou nao tiver elementos, caso contrario,
   *     retornara false.
   */
  public static boolean arrayVazio(Object array) {
    if (array == null) {
      return true;
    }

    return (Array.getLength(array) == 0);
  }

  /**
   * Verifica se uma String contém caracteres e se está instanciada
   *
   * @param s String a ser verificada
   * @return Retorna true se não estiver instanciada ou não tiver caracteres, caso contrário,
   *     retornará false.
   */
  public static boolean StringNullOuVazia(Object s) {
    if (s == null) {
      return true;
    } else {
      return (((String) s).length() == 0);
    }
  }

  /**
   * Verifica se uma String contém caracteres após receber trim() e se está instanciada
   *
   * @param s String a ser verificada
   * @return Retorna true se não estiver instanciada ou não tiver caracteres, caso contrário,
   *     retornará false.
   */
  public static boolean StringNullOuVaziaComTrim(Object s) {
    if (s == null) {
      return true;
    } else {
      return (((String) s).trim().length() == 0);
    }
  }

  /**
   * Verifica se um Integer está instanciado e se seu valor é igual a zero.
   *
   * @param i Integer a ser verificado
   * @return Retorna true se não estiver instanciada ou se seu valor é igual a zero, caso contrário,
   *     retornará false.
   */
  public static boolean IntegerNullOuZero(Object i) {
    if (i == null) {
      return true;
    } else {
      return Integer.parseInt(i.toString()) == 0;
    }
  }

  /**
   * Verifica se um Doble está instanciado e se seu valor é igual a zero.
   *
   * @param i Double a ser verificado
   * @return Retorna true se não estiver instanciada ou se seu valor é igual a zero, caso contrário,
   *     retornará false.
   */
  public static boolean DoubleNullOuZero(Object i) {
    if (i == null) {
      return true;
    } else {
      return Double.parseDouble(i.toString()) == 0D;
    }
  }

  /**
   * Verifica se um Long está instanciado e se seu valor é igual a zero.
   *
   * @param i Long a ser verificado
   * @return Retorna true se não estiver instanciada ou se seu valor é igual a zero, caso contrário,
   *     retornará false.
   */
  public static boolean LongNullOuZero(Object i) {
    if (i == null) {
      return true;
    } else {
      return Long.parseLong(i.toString()) == 0L;
    }
  }

  /**
   * Verifica se um Integer esta instanciado e se seu valor e maior que zero.
   *
   * @param i Integer a ser verificado
   * @return Retorna true se nao estiver instanciada ou se seu valor for menor igual a zero, caso
   *     contrario, retornara false.
   */
  public static boolean IntegerNullOuMenorIgualZero(Integer i) {
    if (i == null) {
      return true;
    } else {
      return (i.intValue() <= 0);
    }
  }

  /**
   * Verifica se um Array de Byte e null ou vazio.
   *
   * @param bytes
   * @return Retorna true se nao estiver instanciada ou se nao tiver nenhum valor, caso contrario,
   *     retornara false.
   */
  public static boolean bytesNullOuVazio(byte[] bytes) {
    if (bytes == null) {
      return true;
    } else {
      return (bytes.length == 0);
    }
  }

  public static byte[] obtemBytesDoArquivo(InputStream is, long length) throws IOException {
    // verifica se o file e null ou um diretorio (pasta)
    // caso seja nao tem como ler uma pasta como sendo um arquivo
    if (is == null) {
      return null;
    }
    byte[] bytes;
    if (length > Integer.MAX_VALUE) {
      // File is too large
      throw new IOException("Arquivo maior do que um vetor java suporta. Integer.MAX_VALUE");
    }
    bytes = new byte[(int) length];
    int offset = 0;
    int numRead = 0;
    while (offset < bytes.length
        && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
      offset += numRead;
    }
    if (offset < bytes.length) {
      throw new IOException("Não foi possível ler todo o arquivo.");
    }
    return bytes;
  }

  /**
   * SALVA O ARQUIVO A PARTIR DOS BYTES
   *
   * @param bytesArquivo
   * @param dst
   * @throws IOException
   */
  public static void saveFile(byte[] bytesArquivo, File dst) throws IOException {

    InputStream in = null;
    OutputStream out = null;
    try {

      // CASO O ARQUIVO DE DESTINO NAO EXISTA
      // CRIA
      if (!dst.exists()) {
        dst.createNewFile();
      }

      in = new ByteArrayInputStream(bytesArquivo);

      // Pega o tamanho do arquivo origem
      long srcLength = bytesArquivo.length;
      long dstLength = 0;

      out = new FileOutputStream(dst);

      // Transfer bytes from in to out
      byte[] buf = new byte[1024];
      int len;
      while ((len = in.read(buf)) > 0) {
        out.write(buf, 0, len);
        // pegando o tamanho do arquivo destino
        dstLength += len;
      }

      // para garantir que todos os bytes foram lidos e gravados
      if (srcLength != dstLength) {
        // CASO NAO TENHA CONSEGUIDO COPIAR
        // todo O ARQUIVO DELETA O NOVO ARQUIVO
        in.close();
        in = null;
        out.close();
        out = null;
        // TENTANDO DELETAR O NOVO ARQUIVO
        // POIS ELE ESTA INCONSISTENTE (NAO FOI COPIADO CORRETAMENTE)
        dst.delete();
      }
    } catch (IOException e) {
      throw new IOException("Não foi possível salvar o arquivo " + dst.getName());
    } finally {

      try {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.flush();
          out.close();
        }
      } catch (IOException e2) {
        throw new IOException("Não foi possível salvar o arquivo " + dst.getName());
      }
    }
  }

  public static String obtemDiretorioPadrao() {
    // PEGANDO OS DIRETORIOS DE GRAVACAO
    ResourceBundle rb = ResourceBundle.getBundle("sistema");
    // VERIFICANDO SE SISTEMA OPERACIONAL E LINUX OU WIN
    String diretorio = rb.getString("pastaRaizLin");
    File dir = new File(diretorio);
    boolean win = !dir.isDirectory();

    // CRIANDO O DIRETORIO PADRAO CASO NAO EXISTA
    if (win) {
      diretorio = rb.getString("pastaRaizSisWin");
    } else {
      diretorio = rb.getString("pastaRaizSisLin");
    }
    dir = new File(diretorio);
    dir.mkdirs();
    return diretorio;
  }

  public static String obtemExtensaoArquivo(String nomeArq) {
    if (StringNullOuVaziaComTrim(nomeArq)) {
      return null;
    }
    int len = nomeArq.lastIndexOf(".");
    if (len == -1) {
      return null;
    }
    if (len + 1 >= nomeArq.length()) {
      return null;
    }
    return nomeArq.substring(len + 1);
  }

  public static boolean isServidorProducao() {
    String ipMaquina = null;
    try {
      ipMaquina = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      ipMaquina = "127.0.0.1";
    }

    ResourceBundle rb = ResourceBundle.getBundle("sistema");
    String ipServidorProducao = rb.getString("ipServidor");

    if (!Utilitario.StringNullOuVaziaComTrim(ipMaquina) && ipMaquina.equals(ipServidorProducao)) {
      return Boolean.TRUE;
    }

    return Boolean.FALSE;
  }

  public static String diretorioLocal() throws IOException {
    File diretorioAtual = new File(".");
    return diretorioAtual.getCanonicalPath();
  }

  public static String[] textToColumns(String line) {

    // Return List
    List<String> list = new ArrayList<>();

    String[] splitBySpace = line.split(" ");

    // Organize the split
    StringBuilder sb = new StringBuilder();
    int initialQuote = 0;
    for (int i = 0; i < splitBySpace.length; i++) {

      // text without space
      if (splitBySpace[i].chars().filter(ch -> ch == '"').count() > 1) {

        list.add(splitBySpace[i]);

        // open the text
      } else if (splitBySpace[i].contains("\"") && initialQuote == 0) {

        initialQuote = 1;
        sb.append(splitBySpace[i]);
        sb.append(" ");

        // close the text
      } else if (splitBySpace[i].contains("\"") && initialQuote == 1) {

        sb.append(splitBySpace[i]);
        list.add(sb.toString());

        initialQuote = 0;
        sb = new StringBuilder();

        // middle of the text
      } else if (initialQuote == 1) {

        sb.append(splitBySpace[i]);
        sb.append(" ");

        // normal without quotes
      } else {

        list.add(splitBySpace[i]);
      }
    }

    // List to Arr
    String[] columns = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      columns[i] = list.get(i);
    }

    return columns;
  }

  public static String[] textToLines(String fullFile) {

    // Return List
    List<String> list = new ArrayList<>();

    fullFile = fullFile.replaceAll("'", "");

    String[] splitByBreakLine;
    if (fullFile.contains("\r\n")) {
      splitByBreakLine = fullFile.split("\r\n");
    } else {
      splitByBreakLine = fullFile.split("\n");
    }

    // Organize the split
    StringBuilder sb = new StringBuilder();
    int initialQuote = 0;
    for (int i = 0; i < splitByBreakLine.length; i++) {

      // text without
      if (splitByBreakLine[i].chars().filter(ch -> ch == '"').count() > 0
          && (splitByBreakLine[i].chars().filter(ch -> ch == '"').count() % 2) == 0) {

        list.add(splitByBreakLine[i]);

        // open the text
      } else if (splitByBreakLine[i].contains("\"") && initialQuote == 0) {

        initialQuote = 1;
        sb.append(splitByBreakLine[i]);
        sb.append(" ");

        // close the text
      } else if (splitByBreakLine[i].contains("\"") && initialQuote == 1) {

        sb.append(splitByBreakLine[i]);
        list.add(sb.toString());

        initialQuote = 0;
        sb = new StringBuilder();

        // middle of the text
      } else if (initialQuote == 1) {

        sb.append(splitByBreakLine[i]);
        sb.append(" ");

        // normal without quotes
      } else {

        list.add(splitByBreakLine[i]);
      }
    }

    // List to Array
    String[] rows = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      rows[i] = list.get(i);
    }

    return rows;
  }

  public static String arrToStr(String[] textLine) {

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < textLine.length; i++) {

      if (i + 1 == textLine.length) {

        sb.append(textLine[i]);
      } else {

        sb.append(textLine[i]);
        sb.append("\t");
      }
    }

    return sb.toString();
  }
}
