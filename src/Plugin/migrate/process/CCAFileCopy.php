<?php

namespace Drupal\cca_file_copy\Plugin\migrate\process;

use Drupal\migrate\MigrateException;
use Drupal\migrate\ProcessPluginBase;
use Drupal\migrate\MigrateExecutableInterface;
use Drupal\migrate\Row;
use Drupal\migrate\Plugin\migrate\process\FileCopy;
use GuzzleHttp\TransferStats;

/**
 * Override FileCopy plugin to add logging and filesize check.
 *
 * @codingStandardsIgnoreStart
 *
 * Examples:
 * @code
 * process:
 *   file:
 *     plugin: cca_file_copy
 *     source: file
 * @endcode
 *
 * @codingStandardsIgnoreEnd
 *
 * @MigrateProcessPlugin(
 *  id = "cca_file_copy"
 * )
 */
class CcaFileCopy extends FileCopy {

  /** @var int Source file size in bytes via Content-Length header */
  private $sourceFilesize;

  /**
   * {@inheritdoc}
   */
  public function transform($value, MigrateExecutableInterface $migrate_executable, Row $row, $destination_property) {
    // If we're stubbing a file entity, return a URI of NULL so it will get
    // stubbed by the general process.
    if ($row->isStub()) {
      return NULL;
    }
    list($source, $destination) = $value;

    $user = \Drupal::currentUser();
    $name = $user->getDisplayName();
    
    // @todo: should change to handle multiple source ids.
    $source_id_key = $row->getSource()['ids'][0];
    $id = $row->getSourceIdValues()[$source_id_key];

    \Drupal::logger('cca_file_copy')->debug(t('file @file requested, row @id by @user', [
      '@file' => $source,
      '@id' => $id,
      '@user' => $name,
    ]));

    // If the source path or URI represents a remote resource, delegate to the
    // download plugin.
    if (!$this->isLocalUri($source)) {
      $resource = $this->downloadPlugin->transform($value, $migrate_executable, $row, $destination_property);

      // Source file size is gathered via Guzzle on_stats but we have no way
      // of retaining it (perhaps we could cache it...)
      try {
        $response = \Drupal::httpClient()->head($source);
        $this->sourceFilesize = $response->getHeader('Content-Length')[0];
      }
      catch (RequestException $e) {
        // noop.
      }

      // Can't load file here as resource not yet inserted in file_managed table.
      // Do HTTP HEAD request against local file via URI.
      $destination_resource = file_create_url($resource);
      $destination_response = \Drupal::httpClient()->head($destination_resource);
      $destination_filesize = $destination_response->getHeader('Content-Length')[0];

      // Trigger migrate exception if entire file not received.
      if ($this->sourceFilesize != $destination_filesize) {
        // Transfer is incomplete, throw exception.
        $source_url_parts = parse_url($source);
        $file_path = $source_url_parts['path'];
        throw new MigrateException(t('Row @id file transfer incomplete: @file Content-Length=@source_filesize, destination filesize=@destination_filesize', [
          '@file' => $file_path,
          '@id' => $id,
          '@source_filesize' => $this->sourceFilesize,
          '@destination_filesize' => $destination_filesize,
        ]));
      }

      return $resource;
    }

    // Ensure the source file exists, if it's a local URI or path.
    if (!file_exists($source)) {
      throw new MigrateException("File '$source' does not exist");
    }

    // If the start and end file is exactly the same, there is nothing to do.
    if ($this->isLocationUnchanged($source, $destination)) {
      return $destination;
    }

    // Check if a writable directory exists, and if not try to create it.
    $dir = $this->getDirectory($destination);
    // If the directory exists and is writable, avoid file_prepare_directory()
    // call and write the file to destination.
    if (!is_dir($dir) || !is_writable($dir)) {
      if (!file_prepare_directory($dir, FILE_CREATE_DIRECTORY | FILE_MODIFY_PERMISSIONS)) {
        throw new MigrateException("Could not create or write to directory '$dir'");
      }
    }

    $final_destination = $this->writeFile($source, $destination, $this->configuration['file_exists']);
    if ($final_destination) {
      return $final_destination;
    }
    throw new MigrateException("File $source could not be copied to $destination");
  }

  /**
   * @param \GuzzleHttp\TransferStats $stats
   *   Post-transfer stats via Guzzle on_stats option.
   */
  public static function onStats(TransferStats $stats){
    \Drupal::logger('cca_file_copy')->info(print_r($stats, TRUE));

    $source_filesize = $stats->getResponse()->getHeader('Content-Length')[0];

    // For some reason Guzzle uses float for content length.
    $destination_filesize = (int)$stats->getHandlerStat('download_content_length');
    if (!empty($source_filesize) && $source_filesize != 0 && $source_filesize != $destination_filesize) {
      $source_url = $stats->getHandlerStat('url');
      $source_url_parts = parse_url($source_url);
      $file_path = $source_url_parts['path'];
      \Drupal::logger('cca_file_copy')->error(t('Incomplete transfer: @file', [
        '@file' => $file_path,
      ]));
    }
  }

}
