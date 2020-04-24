module.exports = class {
  constructor(option) {
    this.option = option;
  }

  log(...messages) {
    if (this.option.debug) {
      console.log(...messages);
    }
  }
};
