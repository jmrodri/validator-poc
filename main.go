package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apimanifests "github.com/operator-framework/api/pkg/manifests"
	apivalidation "github.com/operator-framework/api/pkg/validation"
	registrybundle "github.com/operator-framework/operator-registry/pkg/lib/bundle"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/afero"
	"sigs.k8s.io/yaml"
)

func main() {

	// we expect a single argument which is the bundle root.
	// usage: validator-poc <bundle root>
	if len(os.Args) < 2 {
		fmt.Printf("usage: %s <bundle root>\n", os.Args[0])
		os.Exit(1)
	}

	// Read the bundle object and metadata from the passed in directory.
	bundle, _, err := getBundleDataFromDir(os.Args[1])
	if err != nil {
		fmt.Printf("problem getting bundle [%s] data, %v\n", os.Args[1], err)
		os.Exit(1)
	}

	// TODO: objs is the list of objects in a bundle
	// Pass all exposed bundle objects to the validator, since the underlying
	// validator could filter by type or arbitrary unstructured object keys.
	// NB(estroz): we may also want to pass metadata to these validators,
	// however the set of metadata in a bundle object is not complete
	// (only dependencies, no annotations).
	objs := bundle.ObjectsToValidate()
	for _, obj := range bundle.Objects {
		objs = append(objs, obj)
	}

	// pass the objects to the validator
	results := apivalidation.OperatorHubValidator.Validate(objs...)

	// take each of the ManifestResults and print to STDOUT
	for _, result := range results {
		prettyJSON, err := json.MarshalIndent(result, "", "    ")
		if err != nil {
			// should output JSON so that the call knows how to parse it
			fmt.Printf("XXX ERROR: %v\n", err)
		}
		fmt.Printf("%s\n", string(prettyJSON))
	}
}

// getBundleDataFromDir returns the bundle object and associated metadata from dir, if any.
func getBundleDataFromDir(dir string) (*apimanifests.Bundle, string, error) {
	// Gather bundle metadata.
	metadata, _, err := FindBundleMetadata(dir)
	if err != nil {
		return nil, "", err
	}
	manifestsDirName, hasLabel := metadata.GetManifestsDir()
	if !hasLabel {
		manifestsDirName = registrybundle.ManifestsDir
	}
	manifestsDir := filepath.Join(dir, manifestsDirName)
	// Detect mediaType.
	mediaType, err := registrybundle.GetMediaType(manifestsDir)
	if err != nil {
		return nil, "", err
	}
	// Read the bundle.
	bundle, err := apimanifests.GetBundleFromDir(manifestsDir)
	if err != nil {
		return nil, "", err
	}
	return bundle, mediaType, nil
}

// -------------------------------------------------------
// Everything below this line was copied code from
// internal Operator SDK registry package. We would want
// to make this a library or other reusable code.
// -------------------------------------------------------

type MetadataNotFoundError string

func (e MetadataNotFoundError) Error() string {
	return fmt.Sprintf("metadata not found in %s", string(e))
}

// Labels is a set of key:value labels from an operator-registry object.
type Labels map[string]string

// GetManifestsDir returns the manifests directory name in ls using
// a predefined key, or false if it does not exist.
func (ls Labels) GetManifestsDir() (string, bool) {
	value, hasKey := ls[registrybundle.ManifestsLabel]
	return filepath.Clean(value), hasKey
}

// FindBundleMetadata walks bundleRoot searching for metadata (ex. annotations.yaml),
// and returns metadata and its path if found. If one is not found, an error is returned.
func FindBundleMetadata(bundleRoot string) (Labels, string, error) {
	return findBundleMetadata(afero.NewOsFs(), bundleRoot)
}

func findBundleMetadata(fs afero.Fs, bundleRoot string) (Labels, string, error) {
	// Check the default path first, and return annotations if they were found or an error if that error
	// is not because the path does not exist (it exists or there was an unmarshalling error).
	annotationsPath := filepath.Join(bundleRoot, registrybundle.MetadataDir, registrybundle.AnnotationsFile)
	annotations, err := readAnnotations(fs, annotationsPath)
	if (err == nil && len(annotations) != 0) || (err != nil && !errors.Is(err, os.ErrNotExist)) {
		return annotations, annotationsPath, err
	}

	// Annotations are not at the default path, so search recursively.
	annotations = make(Labels)
	annotationsPath = ""
	err = afero.Walk(fs, bundleRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories and hidden files, or if annotations were already found.
		if len(annotations) != 0 || info.IsDir() || strings.HasPrefix(path, ".") {
			return nil
		}

		annotationsPath = path
		// Ignore this error, since we only care if any annotations are returned.
		if annotations, err = readAnnotations(fs, path); err != nil {
			log.Debug(err)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	if len(annotations) == 0 {
		return nil, "", MetadataNotFoundError(bundleRoot)
	}

	return annotations, annotationsPath, nil
}

// readAnnotations reads annotations from file(s) in bundleRoot and returns them as Labels.
func readAnnotations(fs afero.Fs, annotationsPath string) (Labels, error) {
	// The annotations file is well-defined.
	b, err := afero.ReadFile(fs, annotationsPath)
	if err != nil {
		return nil, err
	}

	// Use the arbitrarily-labelled bundle representation of the annotations file
	// for forwards and backwards compatibility.
	annotations := registrybundle.AnnotationMetadata{
		Annotations: make(Labels),
	}
	if err = yaml.Unmarshal(b, &annotations); err != nil {
		return nil, fmt.Errorf("error unmarshalling potential bundle metadata %s: %v", annotationsPath, err)
	}

	return annotations.Annotations, nil
}
