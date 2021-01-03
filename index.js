const through2 = require(`through2`)
const AWS = require(`aws-sdk`)
const zip = require(`yazl`)

const createParts = (fileBuffer) => {
  let parts = []

  for(let start = 0; start < buffer.length; start += 1024) {
    const end = Math.min(start + 1024, buffer.length)

    const byteSlice = buffer.slice(start, end)

    parts.push(byteSlice)
  }

  return parts
}

const $getVaultName = Symbol(`getVaultName`)

class GlacierStorage {
  constructor(vaultName) {
    this.glacier = new AWS.Glacier()

    this.vaultName = vaultName
  }

  [$getVaultName](ref) {
    return `${this.vaultName}-${ref}`
  }

  async read(ref, opts) {
    // TODO Use Glacier Select

    throw new Error(`Not implemented`)
  }

  async write(ref, entites, opts) {
    const vaultOpts = {
      vaultName: this[$getVaultName](ref)
    }

    let vault = await this.glacier.describeVault(vaultOpts)

    if (!vault) {
      vault = await this.glacier.createVault(vaultOpts)
    }

    const file = new zip.ZipFile()
    entities.forEach((entity) => {
      file.addBuffer(Buffer.from(entity), entity.about.identifier)
    })

    file.end()

    const body = Buffer.alloc(0)

    return new Promise((resolve, reject) => {
      file.outputStream
        .pipe(through2((chunk, _, callback) => {
          body = Buffer.concat([ buff, chunk ])

          return callback()
        }))
        .on('finish', () => {
          let archive

          if (body.length > 1024) {
            let partSize = 1024

            if (body.length > 1024 * 1000) {
              partSize = 1024 * 1000
            } else if (body.length > 1024 * 100) {
              partSize = 1024 * 100
            } else if (body.length > 1024 * 10) {
              partSize = 1024 * 10
            }

            const uploadOpts = Object.assign({
              partSize,
            }, vaultOpts)

            archive = await this.glacier.initiateMultipartUpload(uploadOpts)

            const parts = createParts(body)

            const promises = parts.map((part, index) => {
              const { treeHash: checksum } = this.glacier.computeChecksums(part)

              const partOpts = Object.assign({
                range: `bytes ${index * partSize}-${(index + 1) * partSize}`,
                checksum,
                body: part,
                uploadId: archive.uploadId,
              }, vaultOpts)

              return this.glacier.uploadMultipartPart(partOpts)
            })

            await Promise.all(promises)

            const { treeHash: checksum } = this.glacier.computeChecksums(part)

            const completeOpts = Object.assign({
              checksum,
              archiveSize: body.length,
              uploadId: archive.uploadId,
            }, vaultOpts)

            return this.glacier.completeMultipartUpload(completeOpts)
          } else {
            const { treeHash: checksum } = this.glacier.computeChecksums(body)

            const uploadOpts = Object.assign({
              body,
              checksum,
            }, vaultOpts)

            archive = await this.glacier.uploadArchive()
          }

          return resolve(archive)
        })
    })
  }

  async ls(ref) {
    // TODO Use Glacier Select

    throw new Error(`Not implemented`)
  }

  async rm(ref, entityId) {
    return this.glacier.deleteArchive({
      vaultName: this[$getVaultName](ref),
      archiveId: entityId,
    })
  }
}

module.exports = GlacierStorage
