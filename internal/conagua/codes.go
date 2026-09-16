// Package conagua models CONAGUA's SMN normales-climatológicas catalog:
// the 32 state codes used in URLs, the per-state HTML catalog pages, and the
// station records each page lists.
package conagua

import "fmt"

// BaseURL is the root under which the catalog and all station files live.
// The catalog pages resolve relative anchor hrefs (e.g. "../Diarios/ags/dia01001.txt")
// against the catalog directory; see CatalogURL.
const BaseURL = "https://smn.conagua.gob.mx/tools/RESOURCES/Normales_Climatologicas/"

// StateCode is a CONAGUA state slug (as used in the `estado=` query parameter
// and in the static catalog filename). These are stable across the archive
// and match the dropdown on
// https://smn.conagua.gob.mx/es/climatologia/informacion-climatologica/normales-climatologicas-por-estado.
type StateCode string

// State codes. Order matches the dropdown on the SMN site.
const (
	Aguascalientes  StateCode = "ags"
	BajaCalifornia  StateCode = "bc"
	BajaCaliforniaS StateCode = "bcs"
	Campeche        StateCode = "camp"
	Coahuila        StateCode = "coah"
	Colima          StateCode = "col"
	Chiapas         StateCode = "chis"
	Chihuahua       StateCode = "chih"
	CiudadDeMexico  StateCode = "df"
	Durango         StateCode = "dgo"
	Guanajuato      StateCode = "gto"
	Guerrero        StateCode = "gro"
	Hidalgo         StateCode = "hgo"
	Jalisco         StateCode = "jal"
	EstadoDeMexico  StateCode = "mex"
	Michoacan       StateCode = "mich"
	Morelos         StateCode = "mor"
	Nayarit         StateCode = "nay"
	NuevoLeon       StateCode = "nl"
	Oaxaca          StateCode = "oax"
	Puebla          StateCode = "pue"
	Queretaro       StateCode = "qro"
	QuintanaRoo     StateCode = "qroo"
	SanLuisPotosi   StateCode = "slp"
	Sinaloa         StateCode = "sin"
	Sonora          StateCode = "son"
	Tabasco         StateCode = "tab"
	Tamaulipas      StateCode = "tamps"
	Tlaxcala        StateCode = "tlax"
	Veracruz        StateCode = "ver"
	Yucatan         StateCode = "yuc"
	Zacatecas       StateCode = "zac"
)

// stateNames is the mapping from slug to display name as shown in the SMN
// dropdown. Order is preserved via AllStates.
var stateNames = map[StateCode]string{
	Aguascalientes:  "Aguascalientes",
	BajaCalifornia:  "Baja California",
	BajaCaliforniaS: "Baja California Sur",
	Campeche:        "Campeche",
	Coahuila:        "Coahuila",
	Colima:          "Colima",
	Chiapas:         "Chiapas",
	Chihuahua:       "Chihuahua",
	CiudadDeMexico:  "Ciudad de México",
	Durango:         "Durango",
	Guanajuato:      "Guanajuato",
	Guerrero:        "Guerrero",
	Hidalgo:         "Hidalgo",
	Jalisco:         "Jalisco",
	EstadoDeMexico:  "Estado de México",
	Michoacan:       "Michoacán",
	Morelos:         "Morelos",
	Nayarit:         "Nayarit",
	NuevoLeon:       "Nuevo León",
	Oaxaca:          "Oaxaca",
	Puebla:          "Puebla",
	Queretaro:       "Querétaro",
	QuintanaRoo:     "Quintana Roo",
	SanLuisPotosi:   "San Luis Potosí",
	Sinaloa:         "Sinaloa",
	Sonora:          "Sonora",
	Tabasco:         "Tabasco",
	Tamaulipas:      "Tamaulipas",
	Tlaxcala:        "Tlaxcala",
	Veracruz:        "Veracruz",
	Yucatan:         "Yucatán",
	Zacatecas:       "Zacatecas",
}

// AllStates lists every state code in dropdown order. Callers iterating the
// national catalog should use this slice for a deterministic order.
var AllStates = []StateCode{
	Aguascalientes, BajaCalifornia, BajaCaliforniaS, Campeche, Coahuila,
	Colima, Chiapas, Chihuahua, CiudadDeMexico, Durango, Guanajuato,
	Guerrero, Hidalgo, Jalisco, EstadoDeMexico, Michoacan, Morelos,
	Nayarit, NuevoLeon, Oaxaca, Puebla, Queretaro, QuintanaRoo,
	SanLuisPotosi, Sinaloa, Sonora, Tabasco, Tamaulipas, Tlaxcala,
	Veracruz, Yucatan, Zacatecas,
}

// DisplayName returns the human-readable state name for c, or "" if unknown.
func (c StateCode) DisplayName() string {
	return stateNames[c]
}

// Valid reports whether c is one of the 32 known state codes.
func (c StateCode) Valid() bool {
	_, ok := stateNames[c]
	return ok
}

// CatalogURL returns the absolute URL of the state catalog page for c under
// the production BaseURL.
func (c StateCode) CatalogURL() string {
	return c.CatalogURLUnder(BaseURL)
}

// CatalogURLUnder returns the state catalog page URL for c under base, which
// must end in "/" (as BaseURL does). Pointing base at a local fixture server
// redirects the whole file tree hermetically: the relative hrefs in a catalog
// page resolve against the page URL inside ParseCatalog, so the station-file
// URLs follow the base automatically.
func (c StateCode) CatalogURLUnder(base string) string {
	return base + "catalogo/cat_" + string(c) + ".html"
}

// ParseStateCode validates s and returns the corresponding StateCode.
func ParseStateCode(s string) (StateCode, error) {
	c := StateCode(s)
	if !c.Valid() {
		return "", fmt.Errorf("unknown state code %q (expected one of the 32 CONAGUA slugs; see conagua.AllStates)", s)
	}
	return c, nil
}
