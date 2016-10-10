package eu.fbk.ict.fm.data.util;

import org.apache.commons.cli.Option;

/**
 * Write fucking description!
 *
 * @author Yaroslav Nechaev (remper@me.com)
 */
public class CLIOptionBuilder implements Cloneable {
  /**
   * long option
   */
  private String longopt;

  /**
   * option description
   */
  private String description;

  /**
   * argument name
   */
  private String argName;

  /**
   * is required?
   */
  private boolean required;

  /**
   * the number of arguments
   */
  private int numberOfArgs = Option.UNINITIALIZED;

  /**
   * option type
   */
  private Object type;

  /**
   * option can have an optional argument value
   */
  private boolean optionalArg;

  /**
   * value separator for argument value
   */
  private char valuesep;

  /**
   * Specify a long option value
   *
   * @param newLongopt the long option value
   * @return self
   */
  public CLIOptionBuilder withLongOpt(String newLongopt) {
    longopt = newLongopt;
    return this;
  }

  /**
   * Require an argument value.
   *
   * @return self
   */
  public CLIOptionBuilder hasArg() {
    return this.hasArg(true);
  }

  /**
   * Require an argument value if
   * <code>hasArg</code> is true
   *
   * @param hasArg if true then the Option has an argument value
   * @return self
   */
  public CLIOptionBuilder hasArg(boolean hasArg) {
    numberOfArgs = hasArg ? 1 : Option.UNINITIALIZED;
    return this;
  }

  /**
   * Argument value name
   *
   * @param name the name for the argument value
   * @return self
   */
  public CLIOptionBuilder withArgName(String name) {
    argName = name;

    return this;
  }

  /**
   * Option is required
   *
   * @return self
   */
  public CLIOptionBuilder isRequired() {
    return this.isRequired(true);
  }

  /**
   * The next Option created will be required if <code>required</code>
   * is true.
   *
   * @param newRequired if true then the Option is required
   * @return self
   */
  public CLIOptionBuilder isRequired(boolean newRequired) {
    required = newRequired;
    return this;
  }

  /**
   * Set separator <code>sep</code> as a means to
   * separate argument values
   * <p/>
   * <b>Example:</b>
   * <pre>
   * Option opt = new OptionBuilder().withValueSeparator(':')
   *                           .create('D');
   *
   * CommandLine line = parser.parse(args);
   * String propertyName = opt.getValue(0);
   * String propertyValue = opt.getValue(1);
   * </pre>
   *
   * @param sep The value separator to be used for the argument values
   * @return self
   */
  public CLIOptionBuilder withValueSeparator(char sep) {
    valuesep = sep;
    return this;
  }

  /**
   * Set separator '<code>=</code>' as a means to
   * separate argument values.
   * <p/>
   * <b>Example:</b>
   * <pre>
   * Option opt = new OptionBuilder().withValueSeparator()
   *                           .create('D');
   *
   * CommandLine line = parser.parse(args);
   * String propertyName = opt.getValue(0);
   * String propertyValue = opt.getValue(1);
   * </pre>
   *
   * @return self
   */
  public CLIOptionBuilder withValueSeparator() {
    return this.withValueSeparator('=');
  }

  /**
   * The next Option created can have unlimited argument values
   *
   * @return self
   */
  public CLIOptionBuilder hasArgs() {
    return this.hasArgs(Option.UNLIMITED_VALUES);
  }

  /**
   * Option can have <code>num</code> argument values
   *
   * @param num the number of args that the option can have
   * @return self
   */
  public CLIOptionBuilder hasArgs(int num) {
    numberOfArgs = num;
    return this;
  }

  /**
   * Option can have an optional argument
   *
   * @return self
   */
  public CLIOptionBuilder hasOptionalArg() {
    return this.hasOptionalArgs(1);
  }

  /**
   * Option can have an unlimited number of optional arguments
   *
   * @return the OptionBuilder instance
   */
  public CLIOptionBuilder hasOptionalArgs() {
    optionalArg = true;
    return this.hasArgs();
  }

  /**
   * Option can have the specified number of optional arguments
   *
   * @param numArgs - the maximum number of optional arguments
   *                the next Option created can have.
   * @return self
   */
  public CLIOptionBuilder hasOptionalArgs(int numArgs) {
    optionalArg = true;
    return this.hasArgs(numArgs);
  }

  /**
   * Option will have a value that will be an instance
   * of <code>type</code>
   *
   * @param newType the type of the Options argument value
   * @return self
   */
  public CLIOptionBuilder withType(Object newType) {
    type = newType;
    return this;
  }

  /**
   * Option will have the specified description
   *
   * @param newDescription a description of the Option's purpose
   * @return self
   */
  public CLIOptionBuilder withDescription(String newDescription) {
    description = newDescription;
    return this;
  }

  /**
   * Create an Option using the current settings and with
   * the specified Option <code>char</code>
   *
   * @param opt the character representation of the Option
   * @return the Option instance
   * @throws IllegalArgumentException if <code>opt</code> is not
   *                                  a valid character.  See Option.
   */
  public Option toOption(char opt) throws IllegalArgumentException {
    return toOption(String.valueOf(opt));
  }

  /**
   * Create an Option using the current settings
   *
   * @return the Option instance
   * @throws IllegalArgumentException if <code>longOpt</code> has not been set.
   */
  public Option toOption() throws IllegalArgumentException {
    if (longopt == null) {
      throw new IllegalArgumentException("must specify longopt");
    }

    return toOption(null);
  }

  /**
   * Create an Option using the current settings and with
   * the specified Option <code>char</code>
   *
   * @param opt the <code>java.lang.String</code> representation
   *            of the Option
   * @return the Option instance
   * @throws IllegalArgumentException if <code>opt</code> is not
   *                                  a valid character.  See Option.
   */
  public Option toOption(String opt) throws IllegalArgumentException {
    // create the option
    Option option = new Option(opt, description);

    // set the option properties
    option.setLongOpt(longopt);
    option.setRequired(required);
    option.setOptionalArg(optionalArg);
    option.setArgs(numberOfArgs);
    option.setType(type);
    option.setValueSeparator(valuesep);
    option.setArgName(argName);

    // return the Option instance
    return option;
  }
}
