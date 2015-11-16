package com.datatorrent.lib.io.block;

public class ModuleBlockMetadata extends BlockMetadata.FileBlockMetadata
{

  @Override
  public int hashCode()
  {
    return getFilePath().hashCode();
  }

}
