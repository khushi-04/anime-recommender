document.addEventListener("DOMContentLoaded", () => {

const genreDictionary = [
  "Action", "Adventure", "Cars", "Comedy", "Dementia",
  "Demons", "Drama", "Ecchi", "Fantasy", "Game",
  "Harem", "Historical", "Horror", "Josei", "Kids",
  "Magic", "Martial Arts", "Mecha", "Military", "Music",
  "Mystery", "Parody", "Police", "Psychological", "Romance",
  "Samurai", "School", "Sci-Fi", "Seinen", "Shoujo",
  "Shoujo Ai", "Shounen", "Shounen Ai", "Slice of Life", "Space",
  "Sports", "Super Power", "Supernatural", "Thriller", "Vampire",
  "Yaoi", "Yuri"
];

let animeDictionary = [];
let fuseAnime, fuseGenre;
const selectedAnime  = [];
const selectedGenres = [];

const animeInput = document.getElementById("anime-input");
const animeSuggestions = document.getElementById("anime-suggestions");
const tagsContainer = document.getElementById("selected-anime");

const genreInput = document.getElementById("genre-input");
const genreSuggestions = document.getElementById("genre-suggestions");
const genreTagsContainer = document.getElementById("selected-genres");

const modeButtonsDiv = document.getElementById("mode-buttons");
const btnNormal = document.getElementById("btn-normal");
const btnPersonalize = document.getElementById("btn-personalize");
console.log("btnPersonalize is:", btnPersonalize);


const recListContainer = document.getElementById("recommendations-list");

fetch("/static/js/uniques.json")
  .then((res) => {
    if (!res.ok) throw new Error("Failed to load anime list");
    return res.json();
  })
  .then((data) => {
    animeDictionary = data.title || [];
    fuseAnime = new Fuse(animeDictionary, { threshold: 0.5 });
    fuseGenre = new Fuse(genreDictionary,   { threshold: 0.4 });
    animeInput.addEventListener("input",   updateAnimeSuggestions);
    animeInput.addEventListener("keydown", handleAnimeEnter);

    genreInput.addEventListener("input",   updateGenreSuggestions);
    genreInput.addEventListener("keydown", handleGenreEnter);

    document
      .getElementById("submit-btn")
      .addEventListener("click", handleSubmit);
  })
  .catch((err) => console.error("Error loading anime JSON:", err));

function updateAnimeSuggestions() {
  const q = animeInput.value.trim();
  animeSuggestions.innerHTML = "";
  if (!q) return;

  fuseAnime.search(q).slice(0, 5).forEach(({ item }) => {
    if (!selectedAnime.includes(item)) {
      const li = document.createElement("li");
      li.textContent = item;
      li.addEventListener("click", () => addAnime(item));
      animeSuggestions.appendChild(li);
    }
  });
}

function handleAnimeEnter(e) {
  if (e.key === "Enter" && animeInput.value.trim()) {
    e.preventDefault();
    addAnime(animeInput.value.trim());
  }
}

function addAnime(title) {
  selectedAnime.push(title);
  renderAnimeTags();
  animeInput.value = "";
  animeSuggestions.innerHTML = "";
}

function renderAnimeTags() {
  tagsContainer.innerHTML = "";
  selectedAnime.forEach((t) => {
    const span = document.createElement("span");
    span.className = "tag";
    span.textContent = t;
    tagsContainer.appendChild(span);
  });
}

function updateGenreSuggestions() {
  const q = genreInput.value.trim();
  genreSuggestions.innerHTML = "";
  if (!q) return;

  fuseGenre.search(q).slice(0, 5).forEach(({ item }) => {
    if (!selectedGenres.includes(item)) {
      const li = document.createElement("li");
      li.textContent = item;
      li.addEventListener("click", () => addGenre(item));
      genreSuggestions.appendChild(li);
    }
  });
}

function handleGenreEnter(e) {
  if (e.key === "Enter" && genreInput.value.trim()) {
    e.preventDefault();
    addGenre(genreInput.value.trim());
  }
}

function addGenre(name) {
  selectedGenres.push(name);
  renderGenreTags();
  genreInput.value = "";
  genreSuggestions.innerHTML = "";
}

function renderGenreTags() {
  genreTagsContainer.innerHTML = "";
  selectedGenres.forEach((g) => {
    const span = document.createElement("span");
    span.className = "tag";
    span.textContent = g;
    genreTagsContainer.appendChild(span);
  });
}

function handleSubmit(event) {
  event.preventDefault();
  modeButtonsDiv.style.display = "none";
  recListContainer.innerHTML = "";

  fetch("/api/run-spark", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({
      anime:  selectedAnime,
      genres: selectedGenres
    })
  })
    .then((res) => {
      if (!res.ok) throw new Error("Spark job failed");
      return res.json();
    })
    .then((sparkRes) => {
      console.log("Spark finished:", sparkRes.status);
      modeButtonsDiv.style.display = "block";
    })
    .catch((err) => {
      console.error("Error running Spark:", err);
      alert("Spark job failed: " + err.message);
    });
}


btnNormal.addEventListener("click", () => {
  fetchRecommendations({ mode: "normal" });
});

btnPersonalize.addEventListener("click", () => {
  fetch("/api/get-recommendations", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode: "normal" })
  })
    .then(res => {
      if (!res.ok) throw new Error("Failed to fetch normal recs");
      return res.json();
    })
    .then(normalRes => {
      const recs = normalRes.recommendations || [];
      if (recs.length === 0) {
        throw new Error("No normal recommendations available.");
      }

      const firstCluster = recs[0].cluster;
      console.log("DEBUG: firstCluster =", firstCluster);

      return fetch("/api/get-recommendations", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "personalize", cluster: firstCluster })
      });
    })
    .then(res => {
      if (!res.ok) throw new Error("Failed to fetch personalized recs");
      return res.json();
    })
    .then(personalizedRes => {
      renderRecommendations(personalizedRes.recommendations || []);
    })
    .catch(err => {
      console.error("Error in personalize flow:", err);
      alert("Could not fetch personalized recommendations:\n" + err.message);
    });
});

function fetchRecommendations(payload) {
  console.log("DEBUG: about to POST /api/get-recommendations with payload:", payload);

  fetch("/api/get-recommendations", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  })
    .then(res => {
      console.log("DEBUG: got response status:", res.status, res.statusText);
      return res.text(); 
    })
    .then(text => {
      console.log("DEBUG: raw response body:", text);
      let recRes;
      try {
        recRes = JSON.parse(text);
        console.log("DEBUG: parsed JSON:", recRes);
      } catch (e) {
        throw new Error("Invalid JSON from server: " + e.message);
      }
      return recRes;
    })
    .then(recRes => {
      if (!Array.isArray(recRes.recommendations)) {
        console.warn("DEBUG: recRes.recommendations is not an array:", recRes.recommendations);
      }
      renderRecommendations(recRes.recommendations || []);
    })
    .catch(err => {
      console.error("Error fetching recommendations:", err);
      alert("Failed to get recommendations: " + err.message);
    });
}

function renderRecommendations(list) {
  recListContainer.innerHTML = "";

  if (list.length === 0) {
    recListContainer.textContent = "No recommendations found.";
    return;
  }

  //uploading icon for each genre
  list.forEach((item) => {
    const card = document.createElement("div");
    card.className = "card";
    const genres = item.genre.split(",").map(g => g.trim());
    const firstGenre = genres[0];
    const iconPath = `/static/assets/icons/${firstGenre}.png`;

    card.innerHTML = `
      <img src="${iconPath}" alt="${firstGenre} icon" class="genre-icon-large" />
      <h3 class="anime-title">${item.title}</h3>
      <p>Genre: ${genres.join(", ")}</p>
    `;
    recListContainer.appendChild(card);
  });
}
});