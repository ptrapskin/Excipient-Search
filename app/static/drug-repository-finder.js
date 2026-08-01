(function () {
  "use strict";

  var form = document.getElementById("drfForm");
  var streetInput = document.getElementById("drfStreet");
  var cityInput = document.getElementById("drfCity");
  var stateInput = document.getElementById("drfState");
  var zipInput = document.getElementById("drfZip");
  var statusEl = document.getElementById("drfStatus");
  var resultsEl = document.getElementById("drfResults");

  if (!form) return;

  function buildAddress() {
    var street = streetInput.value.trim();
    var city = cityInput.value.trim();
    var state = stateInput.value.trim();
    var zip = zipInput.value.trim();
    var cityStateZip = [city, [state, zip].filter(Boolean).join(" ")].filter(Boolean).join(", ");
    return [street, cityStateZip].filter(Boolean).join(", ");
  }

  function escapeHtml(value) {
    var div = document.createElement("div");
    div.textContent = value == null ? "" : String(value);
    return div.innerHTML;
  }

  function renderResults(sites) {
    resultsEl.innerHTML = "";
    if (!sites.length) {
      statusEl.textContent = "No participating sites were found.";
      return;
    }
    statusEl.textContent = "Showing " + sites.length + " nearest participating site" + (sites.length === 1 ? "" : "s") + ".";

    sites.forEach(function (site) {
      var li = document.createElement("li");
      li.className = "drf-card";

      var participationLabel = site.participation === "partial"
        ? "Partial participant (transfers donations to a full-participant site)"
        : "Full participant";

      var mapsUrl = "https://www.google.com/maps/search/?api=1&query=" + encodeURIComponent(site.address);

      li.innerHTML =
        '<div class="drf-card-header">' +
          '<h2 class="drf-card-name">' + escapeHtml(site.name) + "</h2>" +
          '<span class="drf-card-distance">' + site.distance_miles + " mi</span>" +
        "</div>" +
        '<p class="drf-card-address"><a href="' + mapsUrl + '" target="_blank" rel="noopener">' + escapeHtml(site.address) + "</a></p>" +
        '<p class="drf-card-meta">' +
          (site.phone ? '<a href="tel:' + escapeHtml(site.phone.replace(/[^\d+]/g, "")) + '">' + escapeHtml(site.phone) + "</a>" : "") +
          (site.contact ? " &middot; Contact: " + escapeHtml(site.contact) : "") +
        "</p>" +
        '<p class="drf-card-participation">' + escapeHtml(participationLabel) + "</p>";

      resultsEl.appendChild(li);
    });
  }

  form.addEventListener("submit", function (event) {
    event.preventDefault();
    var address = buildAddress();
    if (!address) {
      statusEl.textContent = "Enter at least a city, ZIP code, or street address.";
      return;
    }

    statusEl.textContent = "Searching…";
    resultsEl.innerHTML = "";

    fetch("/api/drug-repository-sites/search?address=" + encodeURIComponent(address) + "&limit=10")
      .then(function (response) {
        if (!response.ok) {
          return response.json().then(function (body) {
            throw new Error((body && body.detail) || "Search failed.");
          });
        }
        return response.json();
      })
      .then(function (data) {
        renderResults(data.sites || []);
      })
      .catch(function (err) {
        statusEl.textContent = err.message || "Something went wrong. Please try again.";
      });
  });
})();
