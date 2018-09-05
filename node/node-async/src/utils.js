const crypto = require('crypto');

class Utils {
  static currentTimestamp(isMsec = false) {
    if (isMsec) {
      return Date.now();
    } else {
      return Math.floor(Date.now() / 1000);
    }
  }

  static currentHour() {
    return new Date().getHours();
  }

  static getHash(str) {
    const md5 = crypto.createHash('md5');
    md5.update(str);
    return md5.digest('hex');
  }

  static sleep(time = 0) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, time);
    });
  }
}

module.exports = Utils;
