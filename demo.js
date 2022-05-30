//check for valid email
function isValidEmail(email) {
    var re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(email);
}

//check for valid phone number
function isValidPhone(phone) {
    var re = /^\(?(\d{3})\)?[- ]?(\d{3})[- ]?(\d{4})$/;
    return re.test(phone);
}

//check if user is logged in
function isLoggedIn() {
    var user = Parse.User.current();
    if (user) {
        return true;
    } else {
        return false;
    }
}

//check if user is admin
function isAdmin() {
    var user = Parse.User.current();
    if (user.get("admin")) {
        return true;
    } else {
        return false;
    }
}

//check if button is clicked
function isClicked(button) {
    if (button.hasClass("clicked")) {
        return true;
    } else {
        return false;
    }
}

